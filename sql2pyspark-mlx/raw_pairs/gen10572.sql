WITH remaining_keys AS (
    SELECT category
    FROM products
    INTERSECT
    SELECT category
    FROM products
    WHERE brand NOT IN ('Initech', 'Stark', 'Acme')
),
reconciled AS (
    SELECT s.*
    FROM products s
    INNER JOIN remaining_keys k ON s.category = k.category
)
SELECT
    s.id,
    s.brand,
    s.price,
    s.stock_qty,
    s.supplier_id,
    (SELECT MAX(p2.price) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS max_product_price
FROM reconciled s
ORDER BY brand DESC, price DESC;
