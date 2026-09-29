SELECT id, product_name, price, DATE_TRUNC('YEAR', launch_date) AS launch_date_period FROM products
