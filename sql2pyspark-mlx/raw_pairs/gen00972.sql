SELECT
    id,
    opened_date,
    status,
    LAST_VALUE(status) OVER (ORDER BY opened_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_status
FROM accounts
ORDER BY status DESC, opened_date
