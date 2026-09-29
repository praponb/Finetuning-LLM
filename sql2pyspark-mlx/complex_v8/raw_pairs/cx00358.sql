WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM policies
    WHERE product = 'HOME'
      AND end_date IS NOT NULL
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.product_slug,
        p.holder_name,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM valid s
    JOIN policyholders p ON p.id = s.holder_id
),
holder_segment_stats AS (
    -- Avg premium per holder_segment
    SELECT holder_segment, AVG(premium) AS avg_premium
    FROM combined_details_holder
    GROUP BY holder_segment
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM combined_details_holder s
    JOIN holder_segment_stats g ON g.holder_segment = s.holder_segment
    WHERE s.premium > g.avg_premium
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        product_slug,
        holder_segment,
        holder_risk_score,
        avg_premium,
        premium_minus_avg,
        NTH_VALUE(avg_premium, 2) OVER (ORDER BY holder_risk_score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_avg_premium,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY holder_segment)) AS distinct_holder_id_in_holder_segment,
        COUNT(*) OVER (PARTITION BY holder_segment) AS rows_in_holder_segment,
        MAX(premium_minus_avg) OVER (ORDER BY holder_risk_score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_minus_avg_so_far
    FROM outliers
)
SELECT
    id,
    start_date,
    product_slug,
    holder_segment,
    holder_risk_score,
    avg_premium,
    second_avg_premium,
    distinct_holder_id_in_holder_segment,
    rows_in_holder_segment,
    max_premium_minus_avg_so_far,
    CASE WHEN holder_segment IN ('RETAIL', 'VIP') THEN 'group_a' WHEN holder_segment = 'SME' THEN 'group_b' ELSE 'other' END AS holder_segment_group
FROM with_trends
ORDER BY holder_risk_score DESC, id
LIMIT 5
