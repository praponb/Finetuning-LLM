SELECT *, LOWER(product_name) AS product_name_lower, DATE_FORMAT(launch_date, 'MMM yyyy') AS launch_date_label
FROM products
WHERE category IN ('Home', 'Books', 'Electronics') OR price BETWEEN 690 AND 1020;
