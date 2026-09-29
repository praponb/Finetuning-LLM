WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        YEAR(end_date) AS end_date_year,
        CASE WHEN premium >= 4670.9 THEN 'HIGH' WHEN premium >= 2033.8 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE status NOT IN ('EXPIRED', 'ACTIVE')
      AND YEAR(end_date) = 2026
),
combined_details AS (
    -- Join policyholders
    SELECT s.*, p.risk_score AS holder_risk_score
    FROM filtered s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date_year,
        s.premium_filled,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region
    FROM combined_details s
    JOIN policyholders p ON p.id = s.holder_id AND p.segment <> 'RETAIL'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY holder_region, MONTH(holder_joined_on) ORDER BY end_date_year DESC NULLS FIRST) AS end_date_year_dense_rank,
        COUNT(*) OVER (PARTITION BY holder_region, product) AS rows_in_holder_region_product,
        ROUND(PERCENT_RANK() OVER w_holder_region_holder_joined, 3) AS end_date_year_percent_rank,
        NTH_VALUE(end_date_year, 2) OVER (PARTITION BY holder_region, MONTH(holder_joined_on) ORDER BY end_date_year DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date_year
    FROM with_parent_holder
    WINDOW w_holder_region_holder_joined AS (PARTITION BY holder_region, MONTH(holder_joined_on) ORDER BY end_date_year DESC NULLS FIRST), w_holder_region_product AS (PARTITION BY holder_region, product)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, end_date_year, rows_in_holder_region_product, end_date_year_dense_rank
    FROM windowed
) src
UNPIVOT (
    amount FOR measure IN (end_date_year, rows_in_holder_region_product, end_date_year_dense_rank)
) unp
