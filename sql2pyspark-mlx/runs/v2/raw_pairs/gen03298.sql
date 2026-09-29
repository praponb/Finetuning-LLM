select product_name from products where stock_qty >= 289 and array_contains(tags, 'bestseller') and launch_date between '2024-05-19' and '2024-06-01'
