select id, launch_date, product_name, brand from products where launch_date = (select min(launch_date) from products) order by id desc;
