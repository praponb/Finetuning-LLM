WITH filtered_accounts AS (
    SELECT *, CONCAT_WS('-', account_type, status) AS account_type_status_key
    FROM accounts
    WHERE balance >= 40000 AND opened_date >= '2024-03-01' AND customer_id IS NULL
),
account_type_summary AS (
    SELECT account_type, MIN(opened_date) AS first_opened_date
    FROM filtered_accounts
    GROUP BY account_type
)
SELECT account_type, first_opened_date
FROM account_type_summary
