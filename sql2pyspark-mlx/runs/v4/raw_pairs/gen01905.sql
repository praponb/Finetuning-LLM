SELECT
    id,
    status,
    paid_at,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at, id) AS paid_at_row
FROM payments
ORDER BY paid_at_row
