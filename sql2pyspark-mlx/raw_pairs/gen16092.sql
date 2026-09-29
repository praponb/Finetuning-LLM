WITH enriched AS (
    SELECT
        id,
        account_type,
        balance,
        LEAD(balance) OVER (PARTITION BY account_type, customer_id ORDER BY opened_date, id) AS next_balance,
        LAG(balance) OVER (PARTITION BY account_type, customer_id ORDER BY opened_date, id) AS prev_balance,
        ROW_NUMBER() OVER (PARTITION BY account_type, customer_id ORDER BY opened_date, id) AS seq_no,
        AVG(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS balance_avg_3_prior
    FROM accounts
)
SELECT
    seq_no,
    COUNT_IF(account_type = 'LOAN') AS loan_events,
    APPROX_PERCENTILE(balance_avg_3_prior, 0.95) AS balance_avg_3_prior_p95,
    COUNT(*) AS event_count,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END), 0), 2) AS seq_no_per_loan,
    COUNT(*) AS num_rows
FROM enriched
GROUP BY 1
HAVING balance_avg_3_prior_p95 >= 0
ORDER BY seq_no_per_loan DESC NULLS FIRST
