WITH base AS (
    SELECT *
    FROM products
    WHERE LENGTH(product_name) >= 11 AND category = 'Sports'
),
Summary AS (
    SELECT
        category,
        brand,
        MAX(stock_qty) AS max_stock_qty,
        ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty
    FROM base
    GROUP BY category, brand
)
SELECT category, brand, max_stock_qty
FROM Summary
WHERE stddev_stock_qty >= 45
ORDER BY 1 ASC;
