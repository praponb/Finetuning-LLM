WITH product_summary AS (
    SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    WHERE last_restock BETWEEN '2024-04-15' AND '2024-06-01'
    GROUP BY product_id
)
SELECT p.price, p.category, ps.stddev_quantity
FROM products p
LEFT OUTER JOIN product_summary ps ON p.id = ps.product_id
ORDER BY stddev_quantity DESC
