select product_id, store_id, count(*) as store_sale_count from store_sales group by product_id, store_id having store_sale_count > 5 order by store_sale_count desc, product_id, store_id limit 1
