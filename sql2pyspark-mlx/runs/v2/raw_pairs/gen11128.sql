WITH top_payments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) rnk
    FROM payments
)
SELECT
    id,
    status,
    paid_at,
    amount
FROM top_payments
WHERE rnk <= 5
ORDER BY id DESC;
