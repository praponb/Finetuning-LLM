SELECT
    id,
    opened_date,
    status,
    COALESCE(balance, 0) AS balance_filled
FROM accounts
WHERE opened_date BETWEEN '2024-02-09' AND '2025-07-01' AND (customer_id = 32 OR status IN ('CLOSED', 'FROZEN'));
