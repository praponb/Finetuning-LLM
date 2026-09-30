select id, category, explode(tags) as label from products where month(launch_date) in (1, 5, 6);
