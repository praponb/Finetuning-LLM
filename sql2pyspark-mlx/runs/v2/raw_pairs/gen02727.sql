SELECT id, units, DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label, QUARTER(sale_date) AS sale_date_quarter FROM store_sales WHERE store_id IN (1, 2, 3)
