WITH base AS (
    SELECT *, CAST(balance AS INT) AS balance_int
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND status IS NOT DISTINCT FROM 'OPEN'
)
SELECT
    id,
    account_type,
    balance,
    status,
    MAX(balance) OVER w_part AS last3_max_balance,
    SUM(balance) OVER w_part AS last3_sum_balance,
    AVG(balance) OVER w_part AS last3_avg_balance
FROM base
WINDOW w_part AS (PARTITION BY account_type ORDER BY balance_int, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
