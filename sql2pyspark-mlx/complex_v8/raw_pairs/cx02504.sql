WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        TRUNC(start_date, 'MM') AS start_date_month_start
    FROM policies
    WHERE product IS NOT NULL
      AND status IN ('LAPSED', 'CANCELLED')
),
with_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        s.start_date_month_start,
        p.risk_score AS holder_risk_score,
        p.holder_name,
        p.segment AS holder_segment
    FROM scoped_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, holder_segment ORDER BY start_date), 3) AS start_date_percent_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_id, 0), 4) AS premium_share_of_holder_id,
        DENSE_RANK() OVER (PARTITION BY holder_id, holder_segment ORDER BY start_date) AS start_date_dense_rank,
        ROUND(CUME_DIST() OVER w_holder_id_holder_segment_by_2, 3) AS start_date_cume_dist
    FROM with_details_holder
    WINDOW w_holder_id AS (PARTITION BY holder_id), w_holder_id_holder_segment_by_2 AS (PARTITION BY holder_id, holder_segment ORDER BY start_date, id)
),
per_group AS (
    -- Summarise per group
    SELECT
        holder_id,
        COUNT(DISTINCT holder_segment) AS distinct_holder_segments,
        COUNT(DISTINCT product) AS distinct_products,
        DATEDIFF(MAX(start_date_month_start), MIN(start_date_month_start)) AS start_date_month_start_span_days,
        MIN(start_date_percent_rank) AS min_start_date_percent_rank,
        ROUND(AVG(start_date_cume_dist), 2) AS avg_start_date_cume_dist
    FROM with_windows
    GROUP BY 1
    HAVING MAX(start_date_percent_rank) > 0.3
)
SELECT
    holder_id,
    start_date_month_start_span_days,
    min_start_date_percent_rank,
    avg_start_date_cume_dist
FROM per_group
ORDER BY min_start_date_percent_rank DESC
