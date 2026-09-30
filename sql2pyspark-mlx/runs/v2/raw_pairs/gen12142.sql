select id, product_id, units, ntile(3) over (partition by product_id order by units desc, id) as units_ntile from store_sales;
