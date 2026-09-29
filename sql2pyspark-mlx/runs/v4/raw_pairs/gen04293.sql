select id, category, product_name, explode(tags) as tag from products where month(launch_date) in (3, 9, 11)
