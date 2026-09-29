SELECT
    id,
    brand,
    TRIM(product_name) AS clean_product_name,
    DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date
FROM products
WHERE launch_date < '2024-05-01' AND price < 830
ORDER BY brand DESC;
