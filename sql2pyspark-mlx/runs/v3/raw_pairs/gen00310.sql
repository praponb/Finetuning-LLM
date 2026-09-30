SELECT
    id,
    balance,
    customer_id,
    COALESCE(balance, 0) AS balance_filled
FROM accounts
WHERE MONTH(opened_date) IN (5, 9, 12) AND customer_id IN (19, 25, 27, 36) AND status = 'CLOSED';
