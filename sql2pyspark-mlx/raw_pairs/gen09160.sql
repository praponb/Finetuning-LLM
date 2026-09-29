WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        CAST(stock_qty AS STRING) AS stock_qty_num,
        CASE WHEN price >= 1010 THEN 'High' WHEN price >= 400 THEN 'Medium' ELSE 'Low' END AS price_tier,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM products
    WHERE launch_date > DATE '2023-06-21'
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM cleaned
    EXCEPT ALL
    SELECT id
    FROM products
    WHERE price >= 890
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.id = k.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        price,
        cost,
        stock_qty,
        stock_qty_num,
        price_tier,
        RANK() OVER (ORDER BY cost ASC NULLS FIRST) AS cost_rank
    FROM verified
    WHERE price IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(price, 0) AS price_rounded
    FROM enriched
    WHERE cost_rank > 8
),
enriched_products AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY stock_qty DESC NULLS FIRST) AS stock_qty_dense_rank,
        CUME_DIST() OVER (ORDER BY cost_rank DESC NULLS FIRST) AS cost_rank_cume_dist,
        PERCENT_RANK() OVER (ORDER BY cost_rank ASC NULLS FIRST) AS cost_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY stock_qty DESC NULLS LAST) AS stock_qty_percent_rank
    FROM base
)
SELECT
    price,
    stock_qty,
    stock_qty_num,
    cost_rank,
    stock_qty_dense_rank,
    cost_rank_percent_rank
FROM enriched_products
ORDER BY stock_qty_dense_rank DESC;
