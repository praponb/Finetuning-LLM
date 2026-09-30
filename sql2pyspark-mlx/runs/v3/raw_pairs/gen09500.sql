SELECT id, status, amount
FROM (
    SELECT
        id,
        status,
        amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) rnk
    FROM payments
) ranked
WHERE rnk = 1
ORDER BY amount DESC
