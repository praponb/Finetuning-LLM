select product_id, min(sale_date) as first_sale_date, max(revenue) as max_revenue from store_sales where store_id = 3 group by product_id order by max_revenue asc, product_id desc
