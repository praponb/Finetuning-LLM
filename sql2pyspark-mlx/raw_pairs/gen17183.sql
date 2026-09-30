WITH remaining_keys AS (
    SELECT supplier_id, brand
    FROM products
    INTERSECT ALL
    SELECT supplier_id, brand
    FROM products
    WHERE stock_qty BETWEEN 173 AND 189
),
verified AS (
    SELECT s.*
    FROM products s
    INNER JOIN remaining_keys k ON s.supplier_id = k.supplier_id AND s.brand = k.brand
)
SELECT
    s.id,
    s.category,
    s.brand,
    s.price,
    s.stock_qty,
    s.launch_date,
    s.supplier_id,
    (SELECT MAX(p2.price) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS max_product_price
FROM verified s
ORDER BY category DESC NULLS LAST;
