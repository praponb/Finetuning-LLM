SELECT tags, cost, YEAR(launch_date) AS launch_date_year, LENGTH(brand) AS brand_length FROM products WHERE LENGTH(product_name) > 10
