WITH cleaned AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        tags,
        COALESCE(cost, 0) AS cost_filled
    FROM products
    WHERE brand IS NOT NULL
      AND brand IS DISTINCT FROM 'Initech'
)
SELECT
    id,
    product_name,
    category,
    brand,
    stock_qty,
    tags,
    NTILE(4) OVER (PARTITION BY brand ORDER BY cost ASC, id ASC) AS cost_bucket,
    RANK() OVER (ORDER BY stock_qty ASC NULLS FIRST) AS stock_qty_rank,
    LAG(cost, 2, 0) OVER (PARTITION BY brand, category ORDER BY stock_qty ASC, id ASC) AS prev_cost
FROM cleaned;
