select id, product_id, sale_date, units, lag(units, 1, 0) over (partition by product_id order by sale_date, id) as prev_units from store_sales where product_id in (6, 8);
