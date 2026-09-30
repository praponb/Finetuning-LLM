WITH kept AS (
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.quantity <= 73
        )
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.cost,
    s.tags,
    s.supplier_id,
    CASE WHEN product_name RLIKE '^(Laptop Pro 14|Running Shoes)$' THEN 'CORE' WHEN product_name RLIKE '^(wireless mouse|USB-C Hub)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS product_name_zone,
    (SELECT SUM(x.stock_qty) FROM products x WHERE x.supplier_id = s.supplier_id) AS sum_product_stock_qty
FROM kept s;
