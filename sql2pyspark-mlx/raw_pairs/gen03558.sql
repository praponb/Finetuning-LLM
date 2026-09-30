WITH account_summary AS (
    SELECT account_id, SUM(CASE WHEN merchant = 'Grab' THEN amount ELSE 0 END) AS grab_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    a.opened_date,
    as.grab_amount
FROM accounts a
JOIN account_summary as ON a.id = as.account_id
WHERE as.grab_amount > 31;
