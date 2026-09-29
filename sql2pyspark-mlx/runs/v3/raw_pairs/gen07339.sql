SELECT *
FROM (
    SELECT DATE_TRUNC('MONTH', launch_date) AS launch_month, MIN(launch_date) AS min_launch_date, COUNT(CASE WHEN brand = 'Acme' THEN 1 END) AS acme_count
    FROM products
    WHERE product_name IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', launch_date)
) AS s
WHERE acme_count > 9
ORDER BY acme_count DESC;
