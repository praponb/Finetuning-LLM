SELECT *, DATE_FORMAT(sale_date, 'MMM yyyy') AS sale_date_label, DATE_TRUNC('YEAR', sale_date) AS sale_date_period
FROM store_sales
WHERE units <= 39 AND units < 15;
