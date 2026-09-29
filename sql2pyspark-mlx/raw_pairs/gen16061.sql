SELECT id, category, brand, product_name, DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date FROM products WHERE brand IS NOT NULL;
