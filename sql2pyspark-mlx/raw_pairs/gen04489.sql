WITH account_stats AS (
    SELECT account_id, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list, MIN(amount) AS min_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.account_type,
    a.customer_id,
    as.merchant_list,
    as.min_amount
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id;
