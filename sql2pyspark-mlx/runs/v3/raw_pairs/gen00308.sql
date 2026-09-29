select product_id, max(sale_date) as latest_sale_date from store_sales where units != 20 or store_id in (2, 3, 4, 5) group by product_id
