SELECT
    DATE_FORMAT(ss.sale_date, 'yyyy-MM') AS sale_month,
    ss.product_id,
    MIN(i.last_restock) AS first_last_restock,
    MAX(i.quantity) AS max_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.sale_date BETWEEN '2023-06-01' AND '2024-04-01'
GROUP BY DATE_FORMAT(ss.sale_date, 'yyyy-MM'), ss.product_id
