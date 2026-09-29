WITH ranked AS (
    SELECT
        id,
        status,
        paid_at,
        order_id,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at, id) AS rnk
    FROM payments
)
SELECT *
FROM ranked
WHERE rnk <= 5;
