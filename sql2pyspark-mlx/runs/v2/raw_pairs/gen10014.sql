SELECT
    id,
    supplier_id,
    launch_date,
    brand
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date DESC, id) AS pos
    FROM products
    WHERE supplier_id IS NOT NULL
) AS ranked
WHERE pos = 1;
