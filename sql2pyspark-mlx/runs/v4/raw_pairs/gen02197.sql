WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        rating,
        UPPER(supplier_name) AS supplier_name_upper
    FROM suppliers
    WHERE city IS NOT NULL
)
-- Latest matching product per row (LATERAL ... LIMIT 1)
SELECT s.*, top1.product_category
FROM base s
LEFT JOIN LATERAL (
        SELECT c.category AS product_category
        FROM products c
        WHERE c.supplier_id = s.id
        ORDER BY c.cost DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
