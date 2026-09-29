WITH src AS (
    SELECT *
    FROM products
    WHERE price < 790 AND (LENGTH(product_name) >= 5 OR price BETWEEN 340 AND 1100)
),
products_stats AS (
    SELECT supplier_id, SUM(CASE WHEN brand = 'Initech' THEN stock_qty ELSE 0 END) AS initech_stock_qty
    FROM src
    GROUP BY supplier_id
)
SELECT supplier_id, initech_stock_qty
FROM products_stats
WHERE initech_stock_qty >= 65;
