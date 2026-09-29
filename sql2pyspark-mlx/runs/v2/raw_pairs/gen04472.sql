WITH TopProducts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS pos
    FROM products
    WHERE supplier_id IS NOT NULL
)
SELECT id, category, cost
FROM TopProducts
WHERE pos <= 5;
