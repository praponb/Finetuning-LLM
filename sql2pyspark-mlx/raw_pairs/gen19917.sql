SELECT price, launch_date, price * cost AS margin FROM products WHERE YEAR(launch_date) = 2024 AND product_name LIKE 'Des%';
