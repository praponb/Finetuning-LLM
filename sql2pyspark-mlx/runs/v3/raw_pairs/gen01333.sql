WITH base AS (
    SELECT *, ABS(balance) AS abs_balance
    FROM accounts
    WHERE MONTH(opened_date) IN (8, 10, 11) AND opened_date >= '2024-10-01'
),
accounts_stats AS (
    SELECT customer_id, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM base
    GROUP BY customer_id
)
SELECT customer_id, status_list
FROM accounts_stats;
