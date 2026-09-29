WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY account_type ORDER BY balance) AS rnk
    FROM accounts
    WHERE opened_date BETWEEN '2023-10-01' AND '2023-12-21'
)
SELECT id, account_type, balance
FROM numbered
WHERE rnk = 1;
