SELECT id, store_id, DATE_SUB(sale_date, 37) AS sale_date_minus_37d
FROM store_sales
WHERE sale_date > '2023-08-26'
