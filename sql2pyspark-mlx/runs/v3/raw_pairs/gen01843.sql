SELECT QUARTER(sale_date) AS sale_quarter, MAX(sale_date) AS latest_sale_date FROM store_sales GROUP BY QUARTER(sale_date) ORDER BY sale_quarter ASC LIMIT 5
