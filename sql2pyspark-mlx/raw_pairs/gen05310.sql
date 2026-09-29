WITH matched_keys AS (
    SELECT status
    FROM payments
    EXCEPT
    SELECT status
    FROM payments
    WHERE amount < 1140
),
kept_rows AS (
    SELECT s.*
    FROM payments s
    JOIN matched_keys k ON s.status = k.status
)
SELECT order_id, status
FROM kept_rows
ORDER BY status
