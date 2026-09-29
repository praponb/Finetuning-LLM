select id, product_id, units, max(units) over (partition by product_id) as product_id_max_units from store_sales;
