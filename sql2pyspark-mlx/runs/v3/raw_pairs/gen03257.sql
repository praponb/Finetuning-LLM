SELECT DATE_TRUNC('MONTH', sale_date) sale_month, store_id, COUNT(revenue) revenue_count
FROM store_sales
WHERE units < 13 AND sale_date BETWEEN '2023-12-05' AND '2025-06-01' AND sale_date <= '2023-09-01'
GROUP BY DATE_TRUNC('MONTH', sale_date), store_id;
