select id, launch_date, product_name, cost from products where launch_date < (select min(launch_date) from products) order by launch_date, id;
