WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) rnk
    FROM payments
    WHERE status IN ('SUCCESS', 'REFUNDED')
)
SELECT
    id,
    method,
    paid_at,
    status
FROM Numbered
WHERE rnk <= 5
ORDER BY status DESC;
