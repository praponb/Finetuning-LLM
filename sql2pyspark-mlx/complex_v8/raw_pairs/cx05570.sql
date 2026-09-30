WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT_WS('-', status, product) AS status_product_label,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix
    FROM policies
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM scoped_policys
    WHERE premium < (SELECT MAX(premium) FROM scoped_policys)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY holder_id, MONTH(end_date) ORDER BY start_date DESC, id) AS start_date_quintile,
        LEAD(premium) OVER (PARTITION BY holder_id, MONTH(end_date) ORDER BY start_date DESC, id) AS next_premium,
        MIN(premium) OVER (PARTITION BY holder_id, MONTH(end_date) ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        LAST_VALUE(end_date) OVER (PARTITION BY holder_id, MONTH(end_date) ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        SUM(premium) OVER (PARTITION BY product ORDER BY start_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS premium_sum_1_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product ORDER BY start_date DESC), 3) AS start_date_percent_rank
    FROM filtered
)
-- Subtotals with ROLLUP
SELECT
    product,
    start_date_quintile,
    GROUPING(product) AS is_total_product,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list,
    COUNT(DISTINCT product) AS distinct_products
FROM windowed
GROUP BY ROLLUP(product, start_date_quintile)
