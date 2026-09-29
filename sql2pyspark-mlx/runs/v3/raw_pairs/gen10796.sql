select product_id, store_id from store_sales where month(sale_date) in (4, 9, 10) intersect select product_id, store_id from store_sales where units between 24 and 25 order by store_id desc
