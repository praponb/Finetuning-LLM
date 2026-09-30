WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        DATE_ADD(end_date, 30) AS end_date_plus_30d
    FROM policies
    WHERE start_date BETWEEN DATE '2024-10-22' AND DATE '2025-08-24'
      AND holder_id <= 4
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN premium >= 3544.1 THEN 'large' WHEN premium >= 2111.3 THEN 'medium' ELSE 'small' END AS premium_tier,
        LENGTH(TRIM(product)) AS product_len
    FROM base
    WHERE status IS NOT NULL
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.*,
        p.segment AS holder_segment,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM tagged s
    JOIN policyholders p ON p.id = s.holder_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY premium_tier, holder_segment)) AS distinct_holder_id_in_premium_tier_holder_segmen,
        RANK() OVER w_status_holder_segment_by_hol AS holder_risk_score_rank,
        FIRST_VALUE(holder_risk_score) OVER (PARTITION BY status, holder_segment ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_holder_risk_score,
        SUM(product_len) OVER (PARTITION BY status, holder_segment ORDER BY holder_risk_score DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS product_len_sum_4,
        SUM(holder_risk_score) OVER w_premium_tier_holder_segment AS premium_tier_holder_segment_sum_holder_risk_scor
    FROM joined
    WINDOW w_status_holder_segment_by_hol AS (PARTITION BY status, holder_segment ORDER BY holder_risk_score DESC, id), w_premium_tier_holder_segment AS (PARTITION BY premium_tier, holder_segment)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    status,
    status_slug,
    end_date_plus_30d,
    premium_tier,
    holder_region,
    holder_risk_score_rank,
    product_len_sum_4,
    premium_tier_holder_segment_sum_holder_risk_scor,
    ABS(first_holder_risk_score - 0.6) AS first_holder_risk_score_distance
FROM ranked
ORDER BY first_holder_risk_score_distance NULLS LAST, id
LIMIT 10
