SELECT store_id, SUM(revenue) AS total_revenue, RANK() OVER (ORDER BY SUM(revenue) DESC) AS revenue_rank
FROM store_sales
WHERE YEAR(sale_date) = 2025
GROUP BY store_id
ORDER BY store_id DESC;
