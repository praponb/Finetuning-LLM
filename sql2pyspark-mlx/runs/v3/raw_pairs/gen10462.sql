SELECT
    id,
    status,
    balance,
    opened_date,
    LAST_VALUE(opened_date) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_date
FROM accounts
WHERE status IN ('FROZEN', 'OPEN', 'CLOSED')
