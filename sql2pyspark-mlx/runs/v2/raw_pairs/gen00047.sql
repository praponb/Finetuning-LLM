SELECT
    id,
    brand,
    tags,
    price,
    DATE_FORMAT(launch_date, 'yyyy-MM') launch_date_label
FROM products
WHERE supplier_id IS NOT NULL OR brand IN ('Stark', 'Globex', 'Initech');
