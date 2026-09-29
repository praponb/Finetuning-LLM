select id, product_name, explode(tags) as label from products where month(launch_date) in (2, 3, 8)
