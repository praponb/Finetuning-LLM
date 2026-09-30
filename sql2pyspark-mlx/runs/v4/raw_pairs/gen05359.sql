WITH remaining_keys AS (
    SELECT brand, supplier_id
    FROM products
    EXCEPT ALL
    SELECT brand, supplier_id
    FROM products
    WHERE category = 'Grocery'
),
reconciled AS (
    SELECT s.*
    FROM products s
    INNER JOIN remaining_keys k ON s.brand = k.brand AND s.supplier_id = k.supplier_id
)
SELECT
    product_name,
    brand,
    price,
    cost,
    launch_date,
    CASE WHEN brand RLIKE '^(Globex|Stark)$' THEN 'PRIORITY' WHEN brand RLIKE '^(Acme|Initech)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS brand_zone
FROM reconciled;
