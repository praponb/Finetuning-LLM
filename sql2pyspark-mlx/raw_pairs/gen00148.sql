SELECT product_id, SUM(revenue) AS total_revenue
FROM store_sales
WHERE units BETWEEN 16 AND 29 AND revenue IS NULL
GROUP BY product_id
HAVING SUM(revenue) >= 7000
ORDER BY product_id DESC;
