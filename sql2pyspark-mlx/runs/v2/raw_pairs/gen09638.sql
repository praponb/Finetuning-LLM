SELECT id, revenue, product_id, DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label FROM store_sales WHERE sale_date <= '2024-07-03'
