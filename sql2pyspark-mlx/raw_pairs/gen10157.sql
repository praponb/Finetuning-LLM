WITH filtered_accounts AS (
    SELECT *, LOWER(account_type) account_type_lower
    FROM accounts
    WHERE customer_id IN (1, 15) AND account_type NOT IN ('LOAN', 'SAVINGS')
),
customer_id_summary AS (
    SELECT customer_id, SUM(CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END) credit_count
    FROM filtered_accounts
    GROUP BY customer_id
)
SELECT *
FROM customer_id_summary
