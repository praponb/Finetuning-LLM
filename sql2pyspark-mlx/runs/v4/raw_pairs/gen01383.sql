select id, brand, price, explode(tags) as label from products where year(launch_date) >= 2025
