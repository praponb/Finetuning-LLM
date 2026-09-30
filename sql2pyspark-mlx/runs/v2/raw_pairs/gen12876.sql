select units from store_sales where sale_date between '2023-05-18' and '2024-11-01' union select units from store_sales where product_id in (2, 3, 8)
