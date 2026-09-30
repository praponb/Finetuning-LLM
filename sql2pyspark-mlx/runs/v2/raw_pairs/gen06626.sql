WITH filtered_products AS (
    SELECT *, CONCAT(category, ' - ', product_name) AS category_product_name
    FROM products
    WHERE price BETWEEN 360 AND 1080 AND supplier_id = 33
),
agg AS (
    SELECT category, ROUND(AVG(stock_qty), 2) AS avg_stock_qty
    FROM filtered_products
    GROUP BY category
)
SELECT *
FROM agg
WHERE avg_stock_qty < 67
ORDER BY category DESC;
