WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE status = 'ACTIVE'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id, YEAR(start_date) ORDER BY end_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        ROUND(premium - AVG(premium) OVER w_holder_id_end_date_month, 2) AS premium_vs_holder_id_end_date_month_avg,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date)), 0), 4) AS premium_share_of_holder_id_end_date_month
    FROM recent_policys
    WINDOW w_holder_id_end_date_month AS (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date))
),
qualified AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.segment IS NOT NULL
        )
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DAYOFWEEK(second_end_date) AS second_end_date_dayofweek,
        TRUNC(start_date, 'MM') AS start_date_month_start,
        CASE WHEN premium >= 3521.8 THEN 'HIGH' WHEN premium >= 1541.8 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM qualified
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', second_end_date) AS second_end_date_month,
        holder_id,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        MAX(end_date) AS last_end_date,
        DATEDIFF(MAX(second_end_date), MIN(second_end_date)) AS second_end_date_span_days,
        MAX(premium) AS max_premium,
        MIN(premium_vs_holder_id_end_date_month_avg) AS min_premium_vs_holder_id_end_date_month_avg,
        DATEDIFF(MAX(end_date), MIN(end_date)) AS end_date_span_days,
        ROUND(AVG(premium), 2) AS avg_premium
    FROM enriched
    GROUP BY DATE_TRUNC('MONTH', second_end_date), holder_id, DATE_TRUNC('MONTH', start_date)
),
tagged AS (
    -- Add calculated columns
    SELECT
        second_end_date_month,
        holder_id,
        start_date_month,
        second_end_date_span_days,
        max_premium,
        min_premium_vs_holder_id_end_date_month_avg,
        end_date_span_days,
        ROUND(avg_premium * 1.1, 2) AS avg_premium_adj,
        FLOOR(avg_premium / 1000) * 1000 AS avg_premium_bucket
    FROM summary
),
derived AS (
    -- Add calculated columns
    SELECT
        second_end_date_month,
        holder_id,
        start_date_month,
        second_end_date_span_days,
        max_premium,
        min_premium_vs_holder_id_end_date_month_avg,
        avg_premium_adj,
        avg_premium_bucket + avg_premium_adj AS avg_premium_bucket_plus_avg_premium_adj,
        ROUND(max_premium * 0.9, 2) AS max_premium_adj,
        COALESCE(avg_premium_adj, 0.0) AS avg_premium_adj_filled
    FROM tagged
    WHERE max_premium < 3080.0
)
SELECT
    second_end_date_month,
    holder_id,
    start_date_month,
    second_end_date_span_days,
    min_premium_vs_holder_id_end_date_month_avg,
    avg_premium_bucket_plus_avg_premium_adj,
    avg_premium_adj_filled
FROM derived
