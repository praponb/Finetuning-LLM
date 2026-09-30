SELECT *
FROM (
    SELECT
        account_type,
        customer_id,
        SORT_ARRAY(COLLECT_SET(status)) status_list,
        MAX(balance) max_balance
    FROM accounts
    WHERE opened_date >= '2023-06-01'
    GROUP BY account_type, customer_id
) s
WHERE max_balance < 26000;
