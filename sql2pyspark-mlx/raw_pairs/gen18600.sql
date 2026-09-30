SELECT
    id,
    paid_at,
    status,
    ROW_NUMBER() OVER (PARTITION BY MONTH(paid_at), status ORDER BY paid_at, id) AS paid_at_row
FROM payments
ORDER BY 1 DESC;
