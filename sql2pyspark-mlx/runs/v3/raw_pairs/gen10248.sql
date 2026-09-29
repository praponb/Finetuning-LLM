select id, product_id, store_id, units, rank() over (partition by product_id, store_id order by units) as units_rank from store_sales where units between 19 and 39
