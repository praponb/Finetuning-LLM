WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(country, 1, 3) AS country_prefix
    FROM suppliers
    WHERE city = 'Bangkok'
)
-- Latest matching product per row (LATERAL ... LIMIT 1)
SELECT s.*, top1.launch_date
FROM cleaned s
JOIN LATERAL (
        SELECT c.launch_date
        FROM products c
        WHERE c.supplier_id = s.id
        ORDER BY c.launch_date DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
