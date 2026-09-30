WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status
    FROM policies
    WHERE status IN ('CANCELLED', 'EXPIRED', 'ACTIVE')
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(start_date) OVER (PARTITION BY holder_id, product ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        NTILE(5) OVER (ORDER BY end_date, id) AS end_date_quintile,
        ROUND(PERCENT_RANK() OVER w_all_by_end_date_ties, 3) AS end_date_percent_rank,
        ROW_NUMBER() OVER (ORDER BY end_date, id) AS rn
    FROM base
    WINDOW w_all_by_end_date_ties AS (ORDER BY end_date)
),
latest AS (
    -- Keep the earliest row per product, holder_id
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        first_start_date,
        end_date_quintile,
        end_date_percent_rank,
        rn
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, holder_id ORDER BY end_date_quintile, id) AS recency_rank
        FROM with_trends
    ) ranked
    WHERE recency_rank = 1
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.first_start_date,
        s.end_date_quintile,
        s.end_date_percent_rank,
        s.rn,
        p.holder_name,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM latest s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    product,
    rn,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids,
    MAX(first_start_date) AS last_first_start_date,
    ROUND(100 * COUNT(CASE WHEN LOWER(holder_name) LIKE '%en%' THEN 1 END) / COUNT(*), 1) AS pct_lower_holder_name_like_en
FROM with_details_holder
GROUP BY ROLLUP(product, rn)
