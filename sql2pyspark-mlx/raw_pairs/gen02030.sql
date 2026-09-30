SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, MAX(sale_date) AS latest_sale_date
FROM store_sales
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM')
HAVING AVG(revenue) < 4000;
