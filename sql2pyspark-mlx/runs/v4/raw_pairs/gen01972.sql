SELECT store_id, revenue, QUARTER(sale_date) AS sale_date_quarter
FROM store_sales
WHERE store_id IN (1, 2, 5, 6) AND units > 39 AND revenue > 2700
ORDER BY store_id DESC;
