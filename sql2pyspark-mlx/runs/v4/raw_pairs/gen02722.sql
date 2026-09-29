SELECT
    *,
    LAST_VALUE(status, TRUE) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status,
    AVG(balance) OVER (PARTITION BY account_type, status ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_avg_3,
    ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS seq_no,
    NTILE(10) OVER (PARTITION BY customer_id, status ORDER BY balance DESC, id) AS balance_bucket
FROM accounts;
