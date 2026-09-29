select store_id, product_id, round(revenue * 1.1, 2) as adjusted_revenue from store_sales where sale_date < '2024-05-01' and product_id in (2, 3) and revenue >= 3600
