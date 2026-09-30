select id, store_id, sale_date, row_number() over (partition by store_id order by sale_date, id) as sale_date_row from store_sales;
