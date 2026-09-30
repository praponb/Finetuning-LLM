WITH not_flagged AS (
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.stock_qty >= 364
        )
)
SELECT
    city,
    SUM(CASE WHEN city = 'London' THEN rating ELSE 0 END) AS london_rating,
    COUNT(*) FILTER (WHERE city = 'Singapore') AS singapore_rows
FROM not_flagged
GROUP BY 1
ORDER BY london_rating
