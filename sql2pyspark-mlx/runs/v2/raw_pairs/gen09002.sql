SELECT
    id,
    account_type,
    balance,
    status,
    FIRST_VALUE(status) OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS first_status
FROM accounts
WHERE opened_date BETWEEN '2025-04-01' AND '2025-07-01'
ORDER BY status, 5 DESC;
