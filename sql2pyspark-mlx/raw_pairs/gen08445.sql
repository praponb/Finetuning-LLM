SELECT MONTH(opened_date) AS opened_month, MIN(opened_date) AS first_opened_date, COUNT(*) AS num_stores
FROM stores
WHERE store_name LIKE '%Old%' AND city = 'Bangkok'
GROUP BY MONTH(opened_date)
ORDER BY first_opened_date, opened_month DESC;
