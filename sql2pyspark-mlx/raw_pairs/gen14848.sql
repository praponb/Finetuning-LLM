SELECT id, launch_date, price FROM products WHERE launch_date > (SELECT MAX(launch_date) FROM products) ORDER BY id DESC
