WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(cost * 1.07, 2) AS cost_scaled,
        DAY(delivered_date) AS delivered_date_day,
        COALESCE(carrier, 'OTHER') AS resolved_carrier
    FROM shipments
    WHERE carrier IN ('Kerry', 'Flash')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        carrier,
        cost,
        delivered_date_day,
        resolved_carrier,
        MAX(cost_scaled) OVER w_part AS last4_max_cost_scaled,
        COUNT(cost_scaled) OVER w_part AS last4_count_cost_scaled,
        AVG(cost_scaled) OVER w_part AS last4_avg_cost_scaled,
        AVG(cost) OVER (PARTITION BY YEAR(shipped_date), status ORDER BY cost_scaled, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cost_avg_6_prior
    FROM base
    WHERE cost_scaled < 44
    WINDOW w_part AS (PARTITION BY status ORDER BY delivered_date_day, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(last4_count_cost_scaled, 1) OVER (PARTITION BY carrier ORDER BY last4_count_cost_scaled, id) AS prev_last4_count_cost_scaled,
        MAX(delivered_date_day) OVER (PARTITION BY carrier) AS carrier_max_delivered_date_day,
        ROUND(last4_count_cost_scaled / SUM(last4_count_cost_scaled) OVER (PARTITION BY carrier), 4) AS last4_count_cost_scaled_share,
        LEAD(cost_avg_6_prior, 1) OVER (PARTITION BY carrier ORDER BY last4_count_cost_scaled, id) AS next_cost_avg_6_prior
    FROM metrics
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(prev_last4_count_cost_scaled * 100, 2) AS prev_last4_count_cost_scaled_adjusted,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier_2
    FROM enriched
    WHERE next_cost_avg_6_prior BETWEEN 61 AND 75
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        carrier,
        cost,
        last4_avg_cost_scaled,
        prev_last4_count_cost_scaled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS LAST, id) AS dedup_rank
        FROM valid
    ) t
    WHERE dedup_rank = 1
),
latest_shipments AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        last4_avg_cost_scaled,
        prev_last4_count_cost_scaled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS FIRST, id) AS latest_rank
        FROM latest
    ) ranked
    WHERE latest_rank = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM latest_shipments
    EXCEPT
    SELECT id
    FROM shipments
    WHERE carrier IN ('FedEx', 'Flash', 'Kerry')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_shipments s
    JOIN kept_keys k ON s.id = k.id
)
-- Grouped metrics
SELECT
    CASE WHEN prev_last4_count_cost_scaled >= 12 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_last4_count_cost_scaled_tier,
    ROUND(AVG(prev_last4_count_cost_scaled), 2) AS avg_prev_last4_count_cost_scaled,
    APPROX_PERCENTILE(prev_last4_count_cost_scaled, 0.5) AS prev_last4_count_cost_scaled_p50,
    APPROX_PERCENTILE(last4_avg_cost_scaled, ARRAY(0.25, 0.5, 0.75)) AS last4_avg_cost_scaled_quartiles
FROM verified
GROUP BY CASE WHEN prev_last4_count_cost_scaled >= 12 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING ROUND(AVG(prev_last4_count_cost_scaled), 2) >= 0
