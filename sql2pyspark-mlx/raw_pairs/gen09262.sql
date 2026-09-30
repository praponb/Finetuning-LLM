SELECT *, DATE_TRUNC('week', launch_date) launch_date_period, INITCAP(TRIM(category)) category_title
FROM products
