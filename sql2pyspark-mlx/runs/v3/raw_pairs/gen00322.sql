SELECT
    id,
    supplier_id,
    launch_date,
    RANK() OVER (PARTITION BY supplier_id ORDER BY launch_date) launch_date_rank
FROM products
WHERE brand IN ('Stark', 'Acme', 'Globex')
