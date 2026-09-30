SELECT id, sale_date, ROW_NUMBER() OVER (ORDER BY sale_date, id) AS sale_date_row FROM store_sales;
