SELECT
    id,
    product_name,
    brand,
    cost,
    launch_date,
    supplier_id
FROM products
QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date NULLS FIRST, id) = 1;
