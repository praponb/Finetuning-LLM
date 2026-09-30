WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock < s.launch_date
        )
)
SELECT
    id,
    product_name,
    category,
    price,
    cost,
    launch_date,
    tags,
    supplier_id,
    CASE WHEN product_name RLIKE '^(Laptop Pro 14|Smart Watch)$' THEN 'GROUP_A' WHEN product_name RLIKE '^(wireless mouse|USB-C Hub)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS product_name_zone
FROM not_flagged
