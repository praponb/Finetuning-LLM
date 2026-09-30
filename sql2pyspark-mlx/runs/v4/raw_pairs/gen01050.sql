WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT customer_id
    FROM accounts
    INTERSECT
    SELECT customer_id
    FROM accounts
    WHERE balance BETWEEN 59000 AND 77000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM accounts s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id
)
SELECT
    s.customer_id,
    s.opened_date,
    s.status,
    (SELECT AVG(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS avg_account_balance
FROM kept_rows s
ORDER BY avg_account_balance DESC NULLS LAST, status;
