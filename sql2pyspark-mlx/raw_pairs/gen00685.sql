SELECT
    id,
    supplier_id,
    launch_date,
    ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS rn
FROM products;
