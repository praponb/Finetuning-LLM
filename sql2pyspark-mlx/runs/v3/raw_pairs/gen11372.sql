SELECT product_id, AVG(revenue) AS avg_revenue
FROM store_sales
WHERE sale_date >= '2025-04-27' AND (revenue BETWEEN 1500 AND 2000 OR product_id = 2)
GROUP BY product_id;
