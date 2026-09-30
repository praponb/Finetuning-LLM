SELECT id, product_id, revenue, DATE_SUB(sale_date, 10) AS sale_date_minus_10d, DATE_FORMAT(sale_date, 'MMM yyyy') AS sale_date_label FROM store_sales
