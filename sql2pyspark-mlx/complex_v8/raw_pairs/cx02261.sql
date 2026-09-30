WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE status = 'LAPSED'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY product ORDER BY premium_filled) AS premium_filled_dense_rank,
        COUNT(*) OVER (PARTITION BY YEAR(end_date), product) AS rows_in_end_date_year_product
    FROM scoped_policys
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE premium_filled_dense_rank = 1
      AND premium_filled < 3560.0
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM top_per_group
    WHERE rows_in_end_date_year_product > (SELECT AVG(rows_in_end_date_year_product) FROM top_per_group)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY product, holder_id ORDER BY rows_in_end_date_year_product, id) AS rows_in_end_date_year_product_rank,
        premium_filled_dense_rank - LAG(premium_filled_dense_rank) OVER w_product_holder_id_by_end_dat AS premium_filled_dense_rank_change,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY product, holder_id ORDER BY rows_in_end_date_year_product, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product)) AS distinct_holder_id_in_product,
        ROUND(premium_filled - AVG(premium_filled) OVER (PARTITION BY product), 2) AS premium_filled_vs_product_avg
    FROM screened
    WINDOW w_product_holder_id_by_end_dat AS (PARTITION BY product, holder_id ORDER BY end_date NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    end_date,
    premium_filled,
    rows_in_end_date_year_product,
    rows_in_end_date_year_product_rank,
    premium_filled_dense_rank_change,
    premium_filled_vs_product_avg,
    ROUND(premium_filled / NULLIF(premium, 0), 3) AS premium_filled_per_premium
FROM windowed
WHERE premium_filled_vs_product_avg BETWEEN -3000.0 AND 3000.0
ORDER BY premium_filled_dense_rank_change NULLS LAST
