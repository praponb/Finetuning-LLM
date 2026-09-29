WITH kept_keys AS (
    -- Set intersection on keys
    SELECT store_id, id
    FROM store_sales
    INTERSECT ALL
    SELECT store_id, id
    FROM store_sales
    WHERE sale_date BETWEEN '2024-02-10' AND '2024-04-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        store_id,
        product_id,
        sale_date,
        DENSE_RANK() OVER (PARTITION BY product_id, store_id ORDER BY units DESC NULLS FIRST) units_dense_rank,
        DENSE_RANK() OVER (ORDER BY units NULLS FIRST) units_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY revenue) revenue_dense_rank,
        RANK() OVER (ORDER BY units) units_rank
    FROM kept_rows
    WHERE revenue <= 3600
),
scored AS (
    -- Ranking and gap features
    SELECT
        product_id,
        sale_date,
        RANK() OVER (PARTITION BY store_id ORDER BY units_rank DESC) units_rank_rank,
        ROUND(revenue_dense_rank / SUM(revenue_dense_rank) OVER (PARTITION BY store_id), 4) revenue_dense_rank_share,
        DENSE_RANK() OVER (ORDER BY units_dense_rank_2 DESC NULLS LAST) units_dense_rank_2_dense_rank,
        ROUND(revenue_dense_rank / SUM(revenue_dense_rank) OVER (PARTITION BY YEAR(sale_date)), 4) revenue_dense_rank_share_2,
        ROUND(units_rank / SUM(units_rank) OVER (PARTITION BY store_id, product_id), 4) units_rank_share
    FROM with_windows
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(revenue_dense_rank_share * 0.85, 2) revenue_dense_rank_share_adjusted
    FROM scored
    WHERE units_rank_rank < 6
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    product_id,
    kpi,
    val
FROM (
    SELECT
        product_id,
        units_rank_share,
        revenue_dense_rank_share,
        revenue_dense_rank_share_2,
        revenue_dense_rank_share_adjusted
    FROM filtered
) src
UNPIVOT (
    val FOR kpi IN (units_rank_share AS UNITS_RANK_SHARE, revenue_dense_rank_share, revenue_dense_rank_share_2 AS REVENUE_DENSE_RANK_SHARE_2_VAL, revenue_dense_rank_share_adjusted AS REVENUE_DENSE_RANK_SHARE_ADJUSTED)
)
