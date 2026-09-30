SELECT *, ROUND(revenue, 0) AS revenue_rounded
FROM store_sales
WHERE (sale_date <= '2025-02-01' AND sale_date BETWEEN '2023-12-01' AND '2024-12-01') OR product_id = 2
ORDER BY revenue DESC;
