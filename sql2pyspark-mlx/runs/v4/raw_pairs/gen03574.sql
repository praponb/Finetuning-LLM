WITH filtered AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        TRIM(status) AS clean_status,
        CONCAT(status, ' - ', account_type) AS status_account_type
    FROM accounts
    WHERE opened_date >= DATE '2023-04-28'
)
SELECT
    customer_id,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    STDDEV_SAMP(balance) AS balance_stddev
FROM filtered
GROUP BY customer_id;
