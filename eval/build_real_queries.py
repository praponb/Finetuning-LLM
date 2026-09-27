"""Real-world SQL queries supplied by the user (never used for training).

They run against the hand-made tables in eval/real_tables.py (raw_events,
marketing_campaigns, fraud_blacklist), which are not in the training schema.
Each reference answer is checked against the SQL in Spark before the set is written.

Usage:  python eval/build_real_queries.py     -> eval/real_queries.jsonl
        python eval/evaluate.py --model sql2pyspark:v3 --set eval/real_queries.jsonl --tables eval/real_tables.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "sql2pyspark-mlx"))


def R(name: str, sql: str, body: str, order=(), ref_sql: str | None = None):
    """ref_sql: a Spark-compatible rewrite of `sql` with the same meaning, used only to compute the expected
    result when Spark rejects the original syntax. The model is always given the original `sql`."""
    imports = ["from pyspark.sql import functions as F"]
    if "Window." in body:
        imports.append("from pyspark.sql.window import Window")
    case = {"name": name, "level": "real", "sql": sql.strip(), "code": "\n".join(imports) + "\n\n" + body.strip(),
            "order_cols": list(order)}
    if ref_sql is not None:
        case["ref_sql"] = ref_sql.strip()
    return case


def rewrite(sql: str, *pairs: tuple[str, str]) -> str:
    for old, new in pairs:
        assert old in sql, old
        sql = sql.replace(old, new)
    return sql


Q3 = """
WITH unpacked_telemetry AS (
    -- Complex JSON parsing, schema extraction, and bitwise manipulation
    SELECT 
        t.device_id,
        t.event_timestamp,
        t.firmware_id,
        t.raw_status_mask,
        -- Bitwise check for error flags (e.g., bit 3 set)
        CAST((t.raw_status_mask & 8) != 0 AS INT) AS has_hardware_fault,
        FROM_JSON(
            t.payload_json, 
            'STRUCT<temp: DOUBLE, voltage: DOUBLE, vibration: DOUBLE, metrics_arr: ARRAY<DOUBLE>>'
        ) AS parsed_payload
    FROM sensor_telemetry t
    WHERE t.event_timestamp >= TIMESTAMP '2026-03-01 00:00:00'
      AND t.device_id IS NOT NULL
),
unpivoted_metrics AS (
    -- Native SQL UNPIVOT clause (wide-to-long transformation)
    SELECT 
        device_id,
        event_timestamp,
        firmware_id,
        has_hardware_fault,
        metric_name,
        metric_value
    FROM (
        SELECT 
            device_id,
            event_timestamp,
            firmware_id,
            has_hardware_fault,
            parsed_payload.temp AS temp_celsius,
            parsed_payload.voltage AS supply_volts,
            parsed_payload.vibration AS vibration_hz
        FROM unpacked_telemetry
    ) src
    UNPIVOT (
        metric_value FOR metric_name IN (
            temp_celsius AS 'TEMPERATURE',
            supply_volts AS 'VOLTAGE',
            vibration_hz AS 'VIBRATION'
        )
    )
),
calibrated_telemetry AS (
    -- Correlated LATERAL join fetching the latest valid calibration record per event
    SELECT 
        m.device_id,
        m.event_timestamp,
        m.firmware_id,
        m.has_hardware_fault,
        m.metric_name,
        m.metric_value,
        cal.scale_multiplier,
        cal.calibration_offset,
        (m.metric_value * COALESCE(cal.scale_multiplier, 1.0)) + COALESCE(cal.calibration_offset, 0.0) AS calibrated_value
    FROM unpivoted_metrics m
    JOIN LATERAL (
        SELECT 
            c.scale_multiplier,
            c.calibration_offset
        FROM calibration_registry c
        WHERE c.device_id = m.device_id 
          AND c.metric_name = m.metric_name
          AND c.effective_from <= m.event_timestamp
        ORDER BY c.effective_from DESC
        LIMIT 1
    ) cal ON TRUE
),
stateful_imputation AS (
    -- Advanced windowing with IGNORE NULLS forward-fill, CUME_DIST, and disjoint partitioning
    SELECT 
        ct.device_id,
        ct.event_timestamp,
        ct.firmware_id,
        ct.metric_name,
        ct.has_hardware_fault,
        ct.calibrated_value,
        -- Forward-fill last known good reading over sparse streams
        LAST_VALUE(ct.calibrated_value, TRUE) OVER (
            PARTITION BY ct.device_id, ct.metric_name 
            ORDER BY ct.event_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS imputed_calibrated_value,
        -- Distribution metrics across differing partition boundaries in same query
        CUME_DIST() OVER (
            PARTITION BY ct.metric_name, ct.firmware_id 
            ORDER BY ct.calibrated_value ASC
        ) AS fleet_metric_percentile,
        NTILE(5) OVER (
            PARTITION BY ct.device_id 
            ORDER BY ct.event_timestamp ASC
        ) AS device_chronological_quintile
    FROM calibrated_telemetry ct
),
asymmetric_merge AS (
    -- Full Outer Join handling divergent data streams with COALESCE fallback
    SELECT 
        COALESCE(si.device_id, inc.device_id) AS resolved_device_id,
        COALESCE(si.event_timestamp, inc.incident_time) AS resolved_timestamp,
        si.firmware_id,
        si.metric_name,
        si.imputed_calibrated_value,
        si.fleet_metric_percentile,
        si.has_hardware_fault,
        inc.incident_severity,
        inc.incident_code
    FROM stateful_imputation si
    FULL OUTER JOIN operational_incidents inc
        ON si.device_id = inc.device_id
       AND si.event_timestamp = inc.incident_time
)
-- Map construction, ANSI aggregate FILTER clause, bitwise aggregation, and query hints
SELECT /*+ BROADCAST(meta) */
    am.resolved_device_id,
    DATE_TRUNC('hour', am.resolved_timestamp) AS hour_window,
    meta.region_code,
    -- ANSI SQL aggregate FILTER syntax
    COUNT(am.incident_code) FILTER (WHERE am.incident_severity = 'CRITICAL') AS critical_incident_count,
    AVG(am.imputed_calibrated_value) FILTER (WHERE am.metric_name = 'TEMPERATURE') AS avg_temperature,
    AVG(am.imputed_calibrated_value) FILTER (WHERE am.metric_name = 'VOLTAGE') AS avg_voltage,
    -- Bitwise aggregate over flags
    BIT_OR(COALESCE(am.has_hardware_fault, 0)) AS fleet_fault_encountered,
    -- Map construction from dynamically aggregated arrays
    MAP_FROM_ARRAYS(
        COLLECT_LIST(am.metric_name),
        COLLECT_LIST(ROUND(am.imputed_calibrated_value, 3))
    ) AS latest_readings_map
FROM asymmetric_merge am
LEFT JOIN device_metadata meta
    ON am.resolved_device_id = meta.device_id
GROUP BY 
    am.resolved_device_id,
    DATE_TRUNC('hour', am.resolved_timestamp),
    meta.region_code
HAVING avg_temperature IS NOT NULL 
    OR critical_incident_count > 0;
"""

Q4 = """
WITH parsed_executions AS (
    -- Regex token extraction, safe type casting, and higher-order array transformations
    SELECT 
        trade_id,
        account_id,
        trade_timestamp,
        REGEXP_EXTRACT(raw_symbol, '^([A-Z]{3,5})[/-]([A-Z]{3,5})$', 1) AS base_asset,
        REGEXP_EXTRACT(raw_symbol, '^([A-Z]{3,5})[/-]([A-Z]{3,5})$', 2) AS quote_asset,
        TRY_CAST(execution_details['fill_price'] AS DOUBLE) AS fill_price,
        TRY_CAST(execution_details['quantity'] AS DOUBLE) AS quantity,
        -- Higher-order ZIP_WITH: merge fee rates and tier tiers into itemized costs
        ZIP_WITH(
            fee_tier_rates, 
            applicable_multipliers, 
            (rate, mult) -> ROUND(rate * mult, 6)
        ) AS computed_fee_schedule,
        -- Higher-order AGGREGATE (fold/reduce): sum calculated array elements with seed
        AGGREGATE(
            fee_tier_rates,
            0.0D,
            (acc, rate) -> acc + (rate * TRY_CAST(execution_details['quantity'] AS DOUBLE)),
            acc -> ROUND(acc, 4)
        ) AS total_execution_fee
    FROM trade_stream
    WHERE trade_timestamp >= TIMESTAMP '2026-06-01 00:00:00'
      AND execution_details IS NOT NULL
),
gaps_and_islands AS (
    -- Sessionization island-detection via timestamp step evaluation
    SELECT 
        pe.*,
        CASE 
            WHEN pe.trade_timestamp - LAG(pe.trade_timestamp, 1) OVER (
                PARTITION BY pe.account_id 
                ORDER BY pe.trade_timestamp ASC
            ) > INTERVAL '15' MINUTE 
            THEN 1 
            ELSE 0 
        END AS is_new_burst
    FROM parsed_executions pe
    WHERE pe.fill_price > 0.0 AND pe.quantity > 0.0
),
sessionized_bursts AS (
    -- Secondary cumulative window establishing distinct island session IDs
    SELECT 
        gi.*,
        SUM(gi.is_new_burst) OVER (
            PARTITION BY gi.account_id 
            ORDER BY gi.trade_timestamp ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS burst_island_id
    FROM gaps_and_islands gi
),
statistical_profiles AS (
    -- Reusable named WINDOW specifications, statistical correlation, and standard deviation
    SELECT 
        sb.account_id,
        sb.burst_island_id,
        sb.base_asset,
        sb.quote_asset,
        sb.trade_timestamp,
        sb.fill_price,
        sb.quantity,
        sb.total_execution_fee,
        (sb.fill_price * sb.quantity) - sb.total_execution_fee AS net_consideration,
        -- Statistical functions over defined named windows
        STDDEV_SAMP(sb.fill_price) OVER w_account_burst AS burst_price_stddev,
        CORR(sb.fill_price, sb.quantity) OVER w_account_burst AS price_volume_correlation,
        -- Moving metric over physical millisecond epoch range
        MAX(sb.fill_price) OVER w_sliding_5m AS burst_high_watermark
    FROM sessionized_bursts sb
    WINDOW 
        w_account_burst AS (
            PARTITION BY sb.account_id, sb.burst_island_id 
            ORDER BY sb.trade_timestamp ASC
        ),
        w_sliding_5m AS (
            PARTITION BY sb.account_id, sb.burst_island_id 
            ORDER BY CAST(sb.trade_timestamp AS BIGINT) 
            RANGE BETWEEN 300 PRECEDING AND CURRENT ROW
        )
),
verified_settlements AS (
    -- Bag-semantics multiset intersection
    SELECT account_id, burst_island_id FROM statistical_profiles
    INTERSECT ALL
    SELECT account_id, burst_island_id FROM cleared_settlement_batches
)
-- Multi-dimensional CUBE aggregation, grouping bitmasks, and approximate quantiles
SELECT 
    sp.base_asset,
    sp.quote_asset,
    CASE 
        WHEN sp.burst_price_stddev IS NULL OR sp.burst_price_stddev = 0.0 THEN 'STABLE'
        WHEN sp.burst_price_stddev > 5.0 THEN 'HIGH_VOLATILITY'
        ELSE 'NORMAL'
    END AS volatility_cluster,
    -- Grouping bitmask evaluation across dimensional rollups
    GROUPING_ID(
        sp.base_asset, 
        sp.quote_asset, 
        CASE 
            WHEN sp.burst_price_stddev IS NULL OR sp.burst_price_stddev = 0.0 THEN 'STABLE'
            WHEN sp.burst_price_stddev > 5.0 THEN 'HIGH_VOLATILITY'
            ELSE 'NORMAL'
        END
    ) AS grouping_tier_bitmask,
    COUNT(1) AS total_trades,
    ROUND(SUM(sp.net_consideration), 2) AS aggregate_net_turnover,
    AVG(COALESCE(sp.price_volume_correlation, 0.0)) AS avg_batch_correlation,
    -- Multi-quantile approximation array
    APPROX_PERCENTILE(sp.net_consideration, ARRAY(0.05, 0.50, 0.95), 1000) AS pnl_distribution_quantiles
FROM statistical_profiles sp
INNER JOIN verified_settlements vs
    ON sp.account_id = vs.account_id
   AND sp.burst_island_id = vs.burst_island_id
GROUP BY CUBE (
    sp.base_asset,
    sp.quote_asset,
    CASE 
        WHEN sp.burst_price_stddev IS NULL OR sp.burst_price_stddev = 0.0 THEN 'STABLE'
        WHEN sp.burst_price_stddev > 5.0 THEN 'HIGH_VOLATILITY'
        ELSE 'NORMAL'
    END
)
HAVING GROUPING_ID(
    sp.base_asset, 
    sp.quote_asset, 
    CASE 
        WHEN sp.burst_price_stddev IS NULL OR sp.burst_price_stddev = 0.0 THEN 'STABLE'
        WHEN sp.burst_price_stddev > 5.0 THEN 'HIGH_VOLATILITY'
        ELSE 'NORMAL'
    END
) < 7 -- Suppress high-level aggregate rollups
ORDER BY grouping_tier_bitmask ASC, aggregate_net_turnover DESC NULLS LAST;
"""

HAVING_GROUPING_ID = """HAVING GROUPING_ID(
    sp.base_asset, 
    sp.quote_asset, 
    CASE 
        WHEN sp.burst_price_stddev IS NULL OR sp.burst_price_stddev = 0.0 THEN 'STABLE'
        WHEN sp.burst_price_stddev > 5.0 THEN 'HIGH_VOLATILITY'
        ELSE 'NORMAL'
    END
)"""

CASES = [
    # 2026-09-27: marketing attribution report. CTE chain with dedup (ROW_NUMBER ... NULLS LAST), LAG/LEAD,
    # 30-minute sessionization, a non-equi LEFT JOIN with INTERVAL arithmetic, running / moving window frames,
    # DENSE_RANK, conditional aggregates with NULLIF and HAVING, a correlated NOT EXISTS with a non-equi
    # condition, GROUP BY GROUPING SETS, PERCENT_RANK over an aggregate, and ORDER BY ... NULLS LAST.
    R("marketing_attribution", """
WITH customer_clean AS (
    -- Deduplication & time-gap calculation using window functions
    SELECT 
        user_id,
        session_id,
        event_time,
        event_type,
        amount,
        COALESCE(channel, 'DIRECT') AS acquisition_channel,
        LAG(event_time, 1) OVER (
            PARTITION BY user_id 
            ORDER BY event_time ASC
        ) AS prev_event_time,
        LEAD(event_type, 1) OVER (
            PARTITION BY user_id, session_id 
            ORDER BY event_time ASC
        ) AS next_event_type,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, session_id, event_time, event_type 
            ORDER BY amount DESC NULLS LAST
        ) AS dedup_rank
    FROM raw_events
    WHERE event_time >= TIMESTAMP '2025-01-01 00:00:00'
      AND user_id IS NOT NULL
),
session_attribution AS (
    -- Session boundary identification and non-equi interval joins
    SELECT 
        cc.user_id,
        cc.session_id,
        cc.event_time,
        cc.event_type,
        cc.amount,
        cc.acquisition_channel,
        camp.campaign_id,
        camp.campaign_type,
        CASE 
            WHEN cc.prev_event_time IS NULL 
                 OR cc.event_time > cc.prev_event_time + INTERVAL '30' MINUTE 
            THEN 1 
            ELSE 0 
        END AS is_new_session_flag
    FROM customer_clean cc
    LEFT JOIN marketing_campaigns camp
        ON cc.acquisition_channel = camp.channel_code
       AND cc.event_time >= camp.start_time
       AND cc.event_time < camp.end_time + INTERVAL '7' DAY
    WHERE cc.dedup_rank = 1
),
running_metrics AS (
    -- Multi-frame cumulative aggregates and moving averages
    SELECT 
        user_id,
        session_id,
        event_time,
        event_type,
        amount,
        campaign_id,
        SUM(is_new_session_flag) OVER (
            PARTITION BY user_id 
            ORDER BY event_time ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS calculated_session_id,
        SUM(amount) OVER (
            PARTITION BY user_id 
            ORDER BY event_time ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS lifetime_spend,
        AVG(amount) OVER (
            PARTITION BY user_id 
            ORDER BY event_time ASC 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS moving_avg_3_prior_tx,
        DENSE_RANK() OVER (
            PARTITION BY user_id 
            ORDER BY amount DESC NULLS LAST
        ) AS spend_rank
    FROM session_attribution
),
user_cohorts AS (
    -- Pivot-style conditional aggregation with mathematical transforms
    SELECT 
        user_id,
        COUNT(DISTINCT calculated_session_id) AS total_sessions,
        COUNT(CASE WHEN event_type = 'PURCHASE' THEN 1 END) AS total_purchases,
        SUM(CASE WHEN event_type = 'PURCHASE' THEN amount ELSE 0 END) AS total_revenue,
        MAX(event_time) AS last_active_time,
        ROUND(
            SUM(CASE WHEN event_type = 'PURCHASE' THEN amount ELSE 0 END) / 
            NULLIF(COUNT(CASE WHEN event_type = 'PURCHASE' THEN 1 END), 0), 
            2
        ) AS avg_order_value,
        COLLECT_LIST(DISTINCT campaign_id) AS touched_campaigns
    FROM running_metrics
    GROUP BY user_id
    HAVING COUNT(DISTINCT calculated_session_id) >= 2
)
-- Final aggregation using subquery anti-join, grouping sets, and ranking
SELECT 
    COALESCE(rm.campaign_id, 'ORGANIC') AS resolved_campaign_id,
    DATE_TRUNC('month', rm.event_time) AS activity_month,
    rm.event_type,
    COUNT(DISTINCT rm.user_id) AS unique_active_users,
    SUM(rm.amount) AS total_gross_amount,
    PERCENT_RANK() OVER (
        PARTITION BY DATE_TRUNC('month', rm.event_time) 
        ORDER BY SUM(rm.amount) DESC
    ) AS monthly_revenue_percentile
FROM running_metrics rm
INNER JOIN user_cohorts uc 
    ON rm.user_id = uc.user_id
WHERE rm.spend_rank <= 10
  AND NOT EXISTS (
      -- Anti-join test via correlated subquery
      SELECT 1 
      FROM fraud_blacklist fb 
      WHERE fb.user_id = rm.user_id 
        AND fb.flagged_date <= rm.event_time
  )
GROUP BY GROUPING SETS (
    (DATE_TRUNC('month', rm.event_time), COALESCE(rm.campaign_id, 'ORGANIC'), rm.event_type),
    (DATE_TRUNC('month', rm.event_time), COALESCE(rm.campaign_id, 'ORGANIC')),
    (DATE_TRUNC('month', rm.event_time))
)
ORDER BY activity_month DESC, total_gross_amount DESC NULLS LAST;
""", """
w_prev = Window.partitionBy("user_id").orderBy(F.col("event_time").asc())
w_next = Window.partitionBy("user_id", "session_id").orderBy(F.col("event_time").asc())
w_dedup = Window.partitionBy("user_id", "session_id", "event_time", "event_type").orderBy(F.col("amount").desc_nulls_last())
w_running = (Window.partitionBy("user_id").orderBy(F.col("event_time").asc())
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))
w_prior_3 = Window.partitionBy("user_id").orderBy(F.col("event_time").asc()).rowsBetween(-3, -1)
w_spend = Window.partitionBy("user_id").orderBy(F.col("amount").desc_nulls_last())
w_month = Window.partitionBy("activity_month").orderBy(F.col("total_gross_amount").desc())

customer_clean = (
    df_raw_events
    .filter((F.col("event_time") >= F.lit("2025-01-01 00:00:00").cast("timestamp")) & F.col("user_id").isNotNull())
    .select(
        "user_id",
        "session_id",
        "event_time",
        "event_type",
        "amount",
        F.coalesce(F.col("channel"), F.lit("DIRECT")).alias("acquisition_channel"),
        F.lag("event_time", 1).over(w_prev).alias("prev_event_time"),
        F.lead("event_type", 1).over(w_next).alias("next_event_type"),
        F.row_number().over(w_dedup).alias("dedup_rank"),
    )
)

session_attribution = (
    customer_clean.alias("cc")
    .join(
        df_marketing_campaigns.alias("camp"),
        (F.col("cc.acquisition_channel") == F.col("camp.channel_code"))
        & (F.col("cc.event_time") >= F.col("camp.start_time"))
        & (F.col("cc.event_time") < F.col("camp.end_time") + F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .filter(F.col("cc.dedup_rank") == 1)
    .select(
        F.col("cc.user_id"),
        F.col("cc.session_id"),
        F.col("cc.event_time"),
        F.col("cc.event_type"),
        F.col("cc.amount"),
        F.col("cc.acquisition_channel"),
        F.col("camp.campaign_id"),
        F.col("camp.campaign_type"),
        F.when(
            F.col("cc.prev_event_time").isNull()
            | (F.col("cc.event_time") > F.col("cc.prev_event_time") + F.expr("INTERVAL 30 MINUTES")),
            1,
        ).otherwise(0).alias("is_new_session_flag"),
    )
)

running_metrics = session_attribution.select(
    "user_id",
    "session_id",
    "event_time",
    "event_type",
    "amount",
    "campaign_id",
    F.sum("is_new_session_flag").over(w_running).alias("calculated_session_id"),
    F.sum("amount").over(w_running).alias("lifetime_spend"),
    F.avg("amount").over(w_prior_3).alias("moving_avg_3_prior_tx"),
    F.dense_rank().over(w_spend).alias("spend_rank"),
)

is_purchase = F.col("event_type") == "PURCHASE"
purchase_revenue = F.sum(F.when(is_purchase, F.col("amount")).otherwise(0))
purchase_count = F.count(F.when(is_purchase, 1))

user_cohorts = (
    running_metrics
    .groupBy("user_id")
    .agg(
        F.countDistinct("calculated_session_id").alias("total_sessions"),
        purchase_count.alias("total_purchases"),
        purchase_revenue.alias("total_revenue"),
        F.max("event_time").alias("last_active_time"),
        F.round(purchase_revenue / F.nullif(purchase_count, F.lit(0)), 2).alias("avg_order_value"),
        F.array_distinct(F.collect_list("campaign_id")).alias("touched_campaigns"),
    )
    .filter(F.col("total_sessions") >= 2)
)

result = (
    running_metrics.alias("rm")
    .join(user_cohorts.alias("uc"), F.col("rm.user_id") == F.col("uc.user_id"), "inner")
    .filter(F.col("rm.spend_rank") <= 10)
    .join(
        df_fraud_blacklist.alias("fb"),
        (F.col("fb.user_id") == F.col("rm.user_id")) & (F.col("fb.flagged_date") <= F.col("rm.event_time")),
        "left_anti",
    )
    .select(
        F.coalesce(F.col("rm.campaign_id"), F.lit("ORGANIC")).alias("resolved_campaign_id"),
        F.date_trunc("month", F.col("rm.event_time")).alias("activity_month"),
        F.col("rm.event_type").alias("event_type"),
        F.col("rm.user_id").alias("user_id"),
        F.col("rm.amount").alias("amount"),
    )
    .groupingSets(
        [
            ["activity_month", "resolved_campaign_id", "event_type"],
            ["activity_month", "resolved_campaign_id"],
            ["activity_month"],
        ],
        "resolved_campaign_id",
        "activity_month",
        "event_type",
    )
    .agg(
        F.countDistinct("user_id").alias("unique_active_users"),
        F.sum("amount").alias("total_gross_amount"),
    )
    .withColumn("monthly_revenue_percentile", F.percent_rank().over(w_month))
    .orderBy(F.col("activity_month").desc(), F.col("total_gross_amount").desc_nulls_last())
)
""", ["activity_month", "total_gross_amount"]),
    # 2026-09-27: order fulfillment. Struct field access, LATERAL VIEW [OUTER] EXPLODE of arrays of structs,
    # FILTER / TRANSFORM lambdas, ARRAY_CONTAINS, RANGE frame over epoch seconds, null-safe join (<=>), QUALIFY,
    # EXCEPT DISTINCT, PIVOT with two aliased aggregates, correlated scalar subquery in SELECT, RLIKE.
    R("order_fulfillment", """
WITH parsed_orders AS (
    -- Higher-order array lambdas, struct access, and multi-lateral unnesting
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_timestamp,
        o.shipping_address.country AS dest_country,
        o.shipping_address.postal_code AS dest_postal_code,
        item.sku,
        item.quantity,
        item.unit_price,
        TRANSFORM(
            FILTER(item.applied_discounts, d -> d.amount > 0.0),
            d -> STRUCT(d.discount_code AS code, ROUND(d.amount * 0.9, 2) AS discounted_val)
        ) AS sanitized_discounts,
        wh.warehouse_id,
        wh.distance_km
    FROM orders o
    LATERAL VIEW OUTER EXPLODE(o.line_items) li AS item
    LATERAL VIEW EXPLODE(o.eligible_warehouses) ew AS wh
    WHERE o.order_timestamp >= TIMESTAMP '2026-01-01 00:00:00'
      AND ARRAY_CONTAINS(o.fulfillment_flags, 'PRIORITY')
),
inventory_valuation AS (
    -- Epoch-based RANGE window frames and null-safe equality comparisons
    SELECT 
        po.order_id,
        po.customer_id,
        po.sku,
        po.warehouse_id,
        po.quantity,
        po.unit_price,
        po.dest_country,
        po.order_timestamp,
        inv.batch_id,
        inv.acquisition_cost,
        AVG(inv.acquisition_cost) OVER (
            PARTITION BY po.sku, po.warehouse_id 
            ORDER BY CAST(po.order_timestamp AS BIGINT) 
            RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW -- 7 days in seconds
        ) AS rolling_avg_cost_7d,
        SUM(po.quantity) OVER (
            PARTITION BY po.warehouse_id 
            ORDER BY po.order_timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS warehouse_cumulative_volume
    FROM parsed_orders po
    LEFT JOIN warehouse_inventory inv
        ON po.sku = inv.sku 
       AND po.warehouse_id = inv.warehouse_id
       AND po.dest_country <=> inv.origin_country -- Null-safe join equality
),
eligible_fulfillments AS (
    -- QUALIFY clause window-filtering without subquery wrapping
    SELECT 
        iv.*,
        ROW_NUMBER() OVER (
            PARTITION BY iv.order_id, iv.sku 
            ORDER BY iv.rolling_avg_cost_7d ASC, iv.acquisition_cost ASC NULLS LAST
        ) AS route_rank
    FROM inventory_valuation iv
    QUALIFY route_rank = 1 
        AND warehouse_cumulative_volume > 0
),
reconciled_orders AS (
    -- Set difference across relational slices
    SELECT order_id, sku, warehouse_id FROM eligible_fulfillments
    EXCEPT DISTINCT
    SELECT order_id, sku, warehouse_id FROM blacklisted_fulfillments
),
pivoted_summary AS (
    -- Native SQL PIVOT transformation
    SELECT *
    FROM (
        SELECT 
            ef.dest_country,
            ef.warehouse_id,
            ef.quantity * (ef.unit_price - COALESCE(ef.acquisition_cost, 0)) AS net_margin,
            CASE 
                WHEN ef.quantity >= 10 THEN 'BULK' 
                ELSE 'RETAIL' 
            END AS order_tier
        FROM eligible_fulfillments ef
        INNER JOIN reconciled_orders ro
            ON ef.order_id = ro.order_id 
           AND ef.sku = ro.sku
    ) src
    PIVOT (
        SUM(net_margin) AS total_margin,
        COUNT(1) AS item_count
        FOR order_tier IN ('BULK' AS tier_bulk, 'RETAIL' AS tier_retail)
    )
)
-- Aggregate projection, scalar subquery, and regex pattern matching
SELECT 
    ps.dest_country,
    ps.warehouse_id,
    ps.tier_bulk_total_margin,
    ps.tier_retail_total_margin,
    ps.tier_bulk_item_count + ps.tier_retail_item_count AS total_processed_units,
    ROUND(
        (ps.tier_bulk_total_margin / NULLIF(ps.tier_retail_total_margin, 0)), 
        4
    ) AS bulk_to_retail_margin_ratio,
    (
        -- Correlated scalar subquery in SELECT
        SELECT MAX(sla.dispatch_threshold_hours) 
        FROM warehouse_sla sla 
        WHERE sla.warehouse_id = ps.warehouse_id
    ) AS max_sla_threshold,
    CASE 
        WHEN ps.dest_country RLIKE '^(US|CA|MX)$' THEN 'NAFTA'
        WHEN ps.dest_country RLIKE '^(DE|FR|IT|ES)$' THEN 'EU_CORE'
        ELSE 'REST_OF_WORLD'
    END AS economic_zone
FROM pivoted_summary ps
WHERE (ps.tier_bulk_total_margin > 0 OR ps.tier_retail_total_margin > 0)
  AND ps.dest_country IS NOT NULL
ORDER BY total_processed_units DESC, economic_zone ASC;
""", """
w_rolling_cost = (Window.partitionBy("sku", "warehouse_id")
                  .orderBy(F.col("order_timestamp").cast("bigint"))
                  .rangeBetween(-604800, Window.currentRow))
w_warehouse_volume = (Window.partitionBy("warehouse_id").orderBy("order_timestamp")
                      .rowsBetween(Window.unboundedPreceding, Window.currentRow))
w_route = Window.partitionBy("order_id", "sku").orderBy(
    F.col("rolling_avg_cost_7d").asc(), F.col("acquisition_cost").asc_nulls_last())

parsed_orders = (
    df_orders
    .filter(
        (F.col("order_timestamp") >= F.lit("2026-01-01 00:00:00").cast("timestamp"))
        & F.array_contains("fulfillment_flags", "PRIORITY")
    )
    .withColumn("item", F.explode_outer("line_items"))
    .withColumn("wh", F.explode("eligible_warehouses"))
    .select(
        "order_id",
        "customer_id",
        "order_timestamp",
        F.col("shipping_address.country").alias("dest_country"),
        F.col("shipping_address.postal_code").alias("dest_postal_code"),
        F.col("item.sku").alias("sku"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.unit_price").alias("unit_price"),
        F.transform(
            F.filter("item.applied_discounts", lambda d: d["amount"] > 0.0),
            lambda d: F.struct(d["discount_code"].alias("code"), F.round(d["amount"] * 0.9, 2).alias("discounted_val")),
        ).alias("sanitized_discounts"),
        F.col("wh.warehouse_id").alias("warehouse_id"),
        F.col("wh.distance_km").alias("distance_km"),
    )
)

inventory_valuation = (
    parsed_orders.alias("po")
    .join(
        df_warehouse_inventory.alias("inv"),
        (F.col("po.sku") == F.col("inv.sku"))
        & (F.col("po.warehouse_id") == F.col("inv.warehouse_id"))
        & F.col("po.dest_country").eqNullSafe(F.col("inv.origin_country")),
        "left",
    )
    .select(
        F.col("po.order_id").alias("order_id"),
        F.col("po.customer_id").alias("customer_id"),
        F.col("po.sku").alias("sku"),
        F.col("po.warehouse_id").alias("warehouse_id"),
        F.col("po.quantity").alias("quantity"),
        F.col("po.unit_price").alias("unit_price"),
        F.col("po.dest_country").alias("dest_country"),
        F.col("po.order_timestamp").alias("order_timestamp"),
        F.col("inv.batch_id").alias("batch_id"),
        F.col("inv.acquisition_cost").alias("acquisition_cost"),
    )
    .withColumn("rolling_avg_cost_7d", F.avg("acquisition_cost").over(w_rolling_cost))
    .withColumn("warehouse_cumulative_volume", F.sum("quantity").over(w_warehouse_volume))
)

eligible_fulfillments = (
    inventory_valuation
    .withColumn("route_rank", F.row_number().over(w_route))
    .filter((F.col("route_rank") == 1) & (F.col("warehouse_cumulative_volume") > 0))
)

reconciled_orders = (
    eligible_fulfillments.select("order_id", "sku", "warehouse_id")
    .subtract(df_blacklisted_fulfillments.select("order_id", "sku", "warehouse_id"))
)

pivoted_summary = (
    eligible_fulfillments.alias("ef")
    .join(
        reconciled_orders.alias("ro"),
        (F.col("ef.order_id") == F.col("ro.order_id")) & (F.col("ef.sku") == F.col("ro.sku")),
        "inner",
    )
    .select(
        F.col("ef.dest_country").alias("dest_country"),
        F.col("ef.warehouse_id").alias("warehouse_id"),
        (F.col("ef.quantity") * (F.col("ef.unit_price") - F.coalesce(F.col("ef.acquisition_cost"), F.lit(0))))
        .alias("net_margin"),
        F.when(F.col("ef.quantity") >= 10, "BULK").otherwise("RETAIL").alias("order_tier"),
    )
    .groupBy("dest_country", "warehouse_id")
    .pivot("order_tier", ["BULK", "RETAIL"])
    .agg(F.sum("net_margin").alias("total_margin"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "dest_country",
        "warehouse_id",
        F.col("BULK_total_margin").alias("tier_bulk_total_margin"),
        F.col("BULK_item_count").alias("tier_bulk_item_count"),
        F.col("RETAIL_total_margin").alias("tier_retail_total_margin"),
        F.col("RETAIL_item_count").alias("tier_retail_item_count"),
    )
)

max_sla = df_warehouse_sla.groupBy("warehouse_id").agg(F.max("dispatch_threshold_hours").alias("max_sla_threshold"))

result = (
    pivoted_summary.alias("ps")
    .filter(
        ((F.col("ps.tier_bulk_total_margin") > 0) | (F.col("ps.tier_retail_total_margin") > 0))
        & F.col("ps.dest_country").isNotNull()
    )
    .join(max_sla.alias("sla"), F.col("ps.warehouse_id") == F.col("sla.warehouse_id"), "left")
    .select(
        F.col("ps.dest_country").alias("dest_country"),
        F.col("ps.warehouse_id").alias("warehouse_id"),
        F.col("ps.tier_bulk_total_margin").alias("tier_bulk_total_margin"),
        F.col("ps.tier_retail_total_margin").alias("tier_retail_total_margin"),
        (F.col("ps.tier_bulk_item_count") + F.col("ps.tier_retail_item_count")).alias("total_processed_units"),
        F.round(F.col("ps.tier_bulk_total_margin") / F.nullif(F.col("ps.tier_retail_total_margin"), F.lit(0)), 4)
        .alias("bulk_to_retail_margin_ratio"),
        F.col("sla.max_sla_threshold").alias("max_sla_threshold"),
        F.when(F.col("ps.dest_country").rlike("^(US|CA|MX)$"), "NAFTA")
        .when(F.col("ps.dest_country").rlike("^(DE|FR|IT|ES)$"), "EU_CORE")
        .otherwise("REST_OF_WORLD").alias("economic_zone"),
    )
    .orderBy(F.col("total_processed_units").desc(), F.col("economic_zone").asc())
)
""", ["total_processed_units", "economic_zone"]),
    # 2026-09-27: IoT telemetry. FROM_JSON, bitwise &, UNPIVOT, JOIN LATERAL (... ORDER BY ... LIMIT 1),
    # LAST_VALUE(x, TRUE) (ignore nulls), CUME_DIST, NTILE, FULL OUTER JOIN, BROADCAST hint, aggregate FILTER,
    # BIT_OR, MAP_FROM_ARRAYS(COLLECT_LIST ...), HAVING on aliases.
    # Spark rejects string-literal UNPIVOT aliases (AS 'TEMPERATURE'), so the expected result uses AS TEMPERATURE.
    R("iot_telemetry", Q3, """
payload_schema = "STRUCT<temp: DOUBLE, voltage: DOUBLE, vibration: DOUBLE, metrics_arr: ARRAY<DOUBLE>>"

w_forward_fill = (Window.partitionBy("device_id", "metric_name").orderBy(F.col("event_timestamp").asc())
                  .rowsBetween(Window.unboundedPreceding, Window.currentRow))
w_fleet = Window.partitionBy("metric_name", "firmware_id").orderBy(F.col("calibrated_value").asc())
w_device = Window.partitionBy("device_id").orderBy(F.col("event_timestamp").asc())
w_latest_calibration = Window.partitionBy("device_id", "event_timestamp", "metric_name").orderBy(
    F.col("effective_from").desc())

unpacked_telemetry = (
    df_sensor_telemetry
    .filter(
        (F.col("event_timestamp") >= F.lit("2026-03-01 00:00:00").cast("timestamp"))
        & F.col("device_id").isNotNull()
    )
    .select(
        "device_id",
        "event_timestamp",
        "firmware_id",
        "raw_status_mask",
        (F.col("raw_status_mask").bitwiseAND(8) != 0).cast("int").alias("has_hardware_fault"),
        F.from_json("payload_json", payload_schema).alias("parsed_payload"),
    )
)

unpivoted_metrics = (
    unpacked_telemetry
    .select(
        "device_id",
        "event_timestamp",
        "firmware_id",
        "has_hardware_fault",
        F.col("parsed_payload.temp").alias("TEMPERATURE"),
        F.col("parsed_payload.voltage").alias("VOLTAGE"),
        F.col("parsed_payload.vibration").alias("VIBRATION"),
    )
    .unpivot(
        ["device_id", "event_timestamp", "firmware_id", "has_hardware_fault"],
        ["TEMPERATURE", "VOLTAGE", "VIBRATION"],
        "metric_name",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())      # SQL UNPIVOT excludes NULL values by default
)

# JOIN LATERAL (... ORDER BY effective_from DESC LIMIT 1) ON TRUE -> latest matching calibration per event
calibrated_telemetry = (
    unpivoted_metrics.alias("m")
    .join(
        df_calibration_registry.alias("c"),
        (F.col("c.device_id") == F.col("m.device_id"))
        & (F.col("c.metric_name") == F.col("m.metric_name"))
        & (F.col("c.effective_from") <= F.col("m.event_timestamp")),
        "inner",
    )
    .select(
        F.col("m.device_id").alias("device_id"),
        F.col("m.event_timestamp").alias("event_timestamp"),
        F.col("m.firmware_id").alias("firmware_id"),
        F.col("m.has_hardware_fault").alias("has_hardware_fault"),
        F.col("m.metric_name").alias("metric_name"),
        F.col("m.metric_value").alias("metric_value"),
        F.col("c.scale_multiplier").alias("scale_multiplier"),
        F.col("c.calibration_offset").alias("calibration_offset"),
        F.col("c.effective_from").alias("effective_from"),
    )
    .withColumn("rn", F.row_number().over(w_latest_calibration))
    .filter(F.col("rn") == 1)
    .select(
        "device_id",
        "event_timestamp",
        "firmware_id",
        "has_hardware_fault",
        "metric_name",
        "metric_value",
        "scale_multiplier",
        "calibration_offset",
        (F.col("metric_value") * F.coalesce(F.col("scale_multiplier"), F.lit(1.0))
         + F.coalesce(F.col("calibration_offset"), F.lit(0.0))).alias("calibrated_value"),
    )
)

stateful_imputation = calibrated_telemetry.select(
    "device_id",
    "event_timestamp",
    "firmware_id",
    "metric_name",
    "has_hardware_fault",
    "calibrated_value",
    F.last("calibrated_value", ignorenulls=True).over(w_forward_fill).alias("imputed_calibrated_value"),
    F.cume_dist().over(w_fleet).alias("fleet_metric_percentile"),
    F.ntile(5).over(w_device).alias("device_chronological_quintile"),
)

asymmetric_merge = (
    stateful_imputation.alias("si")
    .join(
        df_operational_incidents.alias("inc"),
        (F.col("si.device_id") == F.col("inc.device_id")) & (F.col("si.event_timestamp") == F.col("inc.incident_time")),
        "full_outer",
    )
    .select(
        F.coalesce(F.col("si.device_id"), F.col("inc.device_id")).alias("resolved_device_id"),
        F.coalesce(F.col("si.event_timestamp"), F.col("inc.incident_time")).alias("resolved_timestamp"),
        F.col("si.firmware_id").alias("firmware_id"),
        F.col("si.metric_name").alias("metric_name"),
        F.col("si.imputed_calibrated_value").alias("imputed_calibrated_value"),
        F.col("si.fleet_metric_percentile").alias("fleet_metric_percentile"),
        F.col("si.has_hardware_fault").alias("has_hardware_fault"),
        F.col("inc.incident_severity").alias("incident_severity"),
        F.col("inc.incident_code").alias("incident_code"),
    )
)

result = (
    asymmetric_merge.alias("am")
    .join(F.broadcast(df_device_metadata).alias("meta"), F.col("am.resolved_device_id") == F.col("meta.device_id"), "left")
    .groupBy(
        F.col("am.resolved_device_id").alias("resolved_device_id"),
        F.date_trunc("hour", F.col("am.resolved_timestamp")).alias("hour_window"),
        F.col("meta.region_code").alias("region_code"),
    )
    .agg(
        F.count(F.when(F.col("am.incident_severity") == "CRITICAL", F.col("am.incident_code")))
        .alias("critical_incident_count"),
        F.avg(F.when(F.col("am.metric_name") == "TEMPERATURE", F.col("am.imputed_calibrated_value")))
        .alias("avg_temperature"),
        F.avg(F.when(F.col("am.metric_name") == "VOLTAGE", F.col("am.imputed_calibrated_value")))
        .alias("avg_voltage"),
        F.bit_or(F.coalesce(F.col("am.has_hardware_fault"), F.lit(0))).alias("fleet_fault_encountered"),
        F.map_from_arrays(
            F.collect_list(F.col("am.metric_name")),
            F.collect_list(F.round(F.col("am.imputed_calibrated_value"), 3)),
        ).alias("latest_readings_map"),
    )
    .filter(F.col("avg_temperature").isNotNull() | (F.col("critical_incident_count") > 0))
)
""", ref_sql=rewrite(Q3, ("AS 'TEMPERATURE'", "AS TEMPERATURE"), ("AS 'VOLTAGE'", "AS VOLTAGE"),
                     ("AS 'VIBRATION'", "AS VIBRATION"))),
    # 2026-09-27: trade analytics. REGEXP_EXTRACT, TRY_CAST of map values, ZIP_WITH / AGGREGATE lambdas,
    # gaps-and-islands with LAG and INTERVAL, named WINDOW clause, STDDEV_SAMP, CORR, RANGE frame, INTERSECT ALL,
    # CUBE over a CASE, GROUPING_ID, APPROX_PERCENTILE with an array of percentages.
    # Spark cannot resolve the repeated GROUPING_ID(... CASE ...) in HAVING, so the expected result uses the alias.
    R("trade_analytics", Q4, """
symbol_pattern = "^([A-Z]{3,5})[/-]([A-Z]{3,5})$"

w_account = Window.partitionBy("account_id").orderBy(F.col("trade_timestamp").asc())
w_islands = (Window.partitionBy("account_id").orderBy(F.col("trade_timestamp").asc())
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))
w_account_burst = Window.partitionBy("account_id", "burst_island_id").orderBy(F.col("trade_timestamp").asc())
w_sliding_5m = (Window.partitionBy("account_id", "burst_island_id")
                .orderBy(F.col("trade_timestamp").cast("bigint"))
                .rangeBetween(-300, Window.currentRow))

quantity = F.col("execution_details")["quantity"].try_cast("double")

parsed_executions = (
    df_trade_stream
    .filter(
        (F.col("trade_timestamp") >= F.lit("2026-06-01 00:00:00").cast("timestamp"))
        & F.col("execution_details").isNotNull()
    )
    .select(
        "trade_id",
        "account_id",
        "trade_timestamp",
        F.regexp_extract("raw_symbol", symbol_pattern, 1).alias("base_asset"),
        F.regexp_extract("raw_symbol", symbol_pattern, 2).alias("quote_asset"),
        F.col("execution_details")["fill_price"].try_cast("double").alias("fill_price"),
        quantity.alias("quantity"),
        F.zip_with("fee_tier_rates", "applicable_multipliers", lambda rate, mult: F.round(rate * mult, 6))
        .alias("computed_fee_schedule"),
        F.aggregate(
            "fee_tier_rates",
            F.lit(0.0),
            lambda acc, rate: acc + rate * quantity,
            lambda acc: F.round(acc, 4),
        ).alias("total_execution_fee"),
    )
)

gaps_and_islands = (
    parsed_executions
    .filter((F.col("fill_price") > 0.0) & (F.col("quantity") > 0.0))
    .withColumn(
        "is_new_burst",
        F.when(
            F.col("trade_timestamp") - F.lag("trade_timestamp", 1).over(w_account) > F.expr("INTERVAL 15 MINUTES"),
            1,
        ).otherwise(0),
    )
)

sessionized_bursts = gaps_and_islands.withColumn("burst_island_id", F.sum("is_new_burst").over(w_islands))

statistical_profiles = sessionized_bursts.select(
    "account_id",
    "burst_island_id",
    "base_asset",
    "quote_asset",
    "trade_timestamp",
    "fill_price",
    "quantity",
    "total_execution_fee",
    (F.col("fill_price") * F.col("quantity") - F.col("total_execution_fee")).alias("net_consideration"),
    F.stddev_samp("fill_price").over(w_account_burst).alias("burst_price_stddev"),
    F.corr("fill_price", "quantity").over(w_account_burst).alias("price_volume_correlation"),
    F.max("fill_price").over(w_sliding_5m).alias("burst_high_watermark"),
)

verified_settlements = (
    statistical_profiles.select("account_id", "burst_island_id")
    .intersectAll(df_cleared_settlement_batches.select("account_id", "burst_island_id"))
)

result = (
    statistical_profiles.alias("sp")
    .join(
        verified_settlements.alias("vs"),
        (F.col("sp.account_id") == F.col("vs.account_id")) & (F.col("sp.burst_island_id") == F.col("vs.burst_island_id")),
        "inner",
    )
    .select(
        F.col("sp.base_asset").alias("base_asset"),
        F.col("sp.quote_asset").alias("quote_asset"),
        F.when(F.col("sp.burst_price_stddev").isNull() | (F.col("sp.burst_price_stddev") == 0.0), "STABLE")
        .when(F.col("sp.burst_price_stddev") > 5.0, "HIGH_VOLATILITY")
        .otherwise("NORMAL").alias("volatility_cluster"),
        F.col("sp.net_consideration").alias("net_consideration"),
        F.col("sp.price_volume_correlation").alias("price_volume_correlation"),
    )
    .cube("base_asset", "quote_asset", "volatility_cluster")
    .agg(
        F.grouping_id().alias("grouping_tier_bitmask"),
        F.count(F.lit(1)).alias("total_trades"),
        F.round(F.sum("net_consideration"), 2).alias("aggregate_net_turnover"),
        F.avg(F.coalesce(F.col("price_volume_correlation"), F.lit(0.0))).alias("avg_batch_correlation"),
        F.percentile_approx("net_consideration", [0.05, 0.50, 0.95], 1000).alias("pnl_distribution_quantiles"),
    )
    .filter(F.col("grouping_tier_bitmask") < 7)
    .orderBy(F.col("grouping_tier_bitmask").asc(), F.col("aggregate_net_turnover").desc_nulls_last())
)
""", ["grouping_tier_bitmask", "aggregate_net_turnover"],
      ref_sql=rewrite(Q4, (HAVING_GROUPING_ID, "HAVING grouping_tier_bitmask"))),
]


def main():
    from verify_pairs import Checker

    import real_tables

    checker = Checker()
    checker.frames.update(real_tables.make_dataframes(checker.spark))
    out = HERE / "real_queries.jsonl"
    with open(out, "w", encoding="utf-8") as f:
        for i, c in enumerate(CASES, 1):
            res = checker.check(c.get("ref_sql") or c["sql"], c["code"], c["order_cols"])
            if not res.ok or not res.n_rows:
                raise SystemExit(f"reference for {c['name']} does not match its SQL: {res}")
            f.write(json.dumps({"id": f"real{i:03d}", **c}, ensure_ascii=False) + "\n")
            print(f"  real{i:03d} {c['name']}: reference ok ({res.n_rows} rows)")
    print(f"wrote {len(CASES)} cases to {out}")


if __name__ == "__main__":
    main()
