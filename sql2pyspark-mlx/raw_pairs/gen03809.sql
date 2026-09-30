WITH cleaned AS (
    SELECT *, CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END AS is_starbucks
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-06-01 14:00:00'
      AND amount > 2800
),
enriched AS (
    SELECT
        a.id,
        a.account_id,
        a.merchant,
        d.account_type
    FROM cleaned a
    LEFT JOIN accounts d
        ON a.account_id = d.id
),
joined AS (
    SELECT
        e.id,
        e.account_id,
        e.merchant,
        e.account_type,
        d.status
    FROM enriched e
    LEFT JOIN accounts d
        ON e.account_id = d.id
),
joined_transactions AS (
    SELECT
        s.*,
        d.opened_date,
        d.status AS account_status
    FROM joined s
    LEFT JOIN accounts d
        ON s.account_id = d.id
),
enriched_transactions AS (
    SELECT s.*, p.balance
    FROM joined_transactions s
    LEFT JOIN accounts p
        ON s.account_id = p.id
    WHERE MONTH(p.opened_date) IN (4, 8, 11)
),
with_windows AS (
    SELECT
        id,
        account_type,
        status,
        opened_date,
        balance,
        LEAD(balance) OVER w_part AS next_balance,
        LAG(balance) OVER w_part AS prev_balance,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM enriched_transactions
    WINDOW w_part AS (PARTITION BY account_status, account_id ORDER BY balance ASC, id ASC)
),
with_windows_transactions AS (
    SELECT
        id,
        status,
        opened_date,
        AVG(balance) OVER (PARTITION BY YEAR(opened_date), account_type ORDER BY opened_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS balance_avg_2_prior,
        RANK() OVER (PARTITION BY YEAR(opened_date), status ORDER BY next_balance ASC NULLS LAST) AS next_balance_rank,
        SUM(balance) OVER w_running AS cumulative_balance
    FROM with_windows
    WINDOW w_running AS (ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    ROUND(AVG(balance_avg_2_prior), 2) AS avg_balance_avg_2_prior,
    MIN(balance_avg_2_prior) AS min_balance_avg_2_prior,
    SIZE(COLLECT_SET(status)) AS num_status,
    APPROX_PERCENTILE(next_balance_rank, 0.95) AS next_balance_rank_p95
FROM with_windows_transactions
GROUP BY DATE_TRUNC('YEAR', opened_date)
