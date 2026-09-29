select id, store_id, product_id, units, dense_rank() over (partition by store_id, product_id order by units desc) as units_dense_rank from store_sales where revenue <= 1900
