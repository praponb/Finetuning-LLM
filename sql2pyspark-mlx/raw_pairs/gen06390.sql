WITH kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM products
    INTERSECT ALL
    SELECT id
    FROM products
    WHERE product_name IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM products s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    s.product_name,
    s.price,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.tags,
    s.supplier_id,
    (SELECT SUM(t2.cost) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS sum_product_cost
FROM reconciled s
ORDER BY cost DESC NULLS LAST, price DESC;
