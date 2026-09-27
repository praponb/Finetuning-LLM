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


def R(name: str, sql: str, body: str, order=()):
    imports = ["from pyspark.sql import functions as F"]
    if "Window." in body:
        imports.append("from pyspark.sql.window import Window")
    return {"name": name, "level": "real", "sql": sql.strip(), "code": "\n".join(imports) + "\n\n" + body.strip(),
            "order_cols": list(order)}


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
]


def main():
    from verify_pairs import Checker

    import real_tables

    checker = Checker()
    checker.frames.update(real_tables.make_dataframes(checker.spark))
    out = HERE / "real_queries.jsonl"
    with open(out, "w", encoding="utf-8") as f:
        for i, c in enumerate(CASES, 1):
            res = checker.check(c["sql"], c["code"], c["order_cols"])
            if not res.ok or not res.n_rows:
                raise SystemExit(f"reference for {c['name']} does not match its SQL: {res}")
            f.write(json.dumps({"id": f"real{i:03d}", **c}, ensure_ascii=False) + "\n")
            print(f"  real{i:03d} {c['name']}: reference ok ({res.n_rows} rows)")
    print(f"wrote {len(CASES)} cases to {out}")


if __name__ == "__main__":
    main()
