SELECT
    id,
    brand,
    tags,
    DATE_FORMAT(launch_date, 'yyyy-MM-dd') AS launch_date_label
FROM products
WHERE brand = 'Initech';
