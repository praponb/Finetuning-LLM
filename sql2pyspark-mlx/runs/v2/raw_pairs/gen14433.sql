select id, product_id, revenue, units, datediff(current_date(), sale_date) as days_since_sale_date from store_sales where (revenue <= 2100 and revenue > 4000) or product_id in (7, 9)
