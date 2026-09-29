SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, COUNT(units) AS units_count
FROM store_sales
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM');
