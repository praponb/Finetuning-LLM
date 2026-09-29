WITH base AS (
    SELECT *
    FROM accounts
    WHERE opened_date > '2024-02-01' AND status IN ('OPEN', 'FROZEN', 'CLOSED')
),
summary AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values, COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count
    FROM base
    GROUP BY customer_id
)
SELECT customer_id, account_type_values, frozen_count
FROM summary
ORDER BY account_type_values DESC, customer_id DESC;
