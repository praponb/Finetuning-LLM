WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        CONCAT(account_type, ' - ', status) AS account_type_status,
        ABS(balance) AS abs_balance,
        COALESCE(status, 'NONE') AS status_filled
    FROM accounts
    WHERE status IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY abs_balance DESC, id) AS row_num
    FROM valid
    QUALIFY row_num = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(balance) OVER w_recent AS running_count_balance,
        MAX(balance) OVER w_recent AS running_max_balance,
        AVG(balance) OVER w_recent AS running_avg_balance
    FROM deduped
    WINDOW w_recent AS (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(running_max_balance, 1) OVER (PARTITION BY YEAR(opened_date), customer_id ORDER BY running_avg_balance, id) AS next_running_max_balance,
        SUM(abs_balance) OVER (PARTITION BY customer_id, account_type) AS customer_id_sum_abs_balance,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY row_num, id) AS seq_no,
        LAG(running_avg_balance, 1) OVER (ORDER BY running_avg_balance, id) AS prev_running_avg_balance,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY running_avg_balance DESC NULLS FIRST) AS running_avg_balance_dense_rank
    FROM metrics
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(customer_id_sum_abs_balance, 1, 0) OVER (PARTITION BY customer_id ORDER BY abs_balance, id) AS next_customer_id_sum_abs_balance,
        AVG(seq_no) OVER (PARTITION BY YEAR(opened_date), customer_id ORDER BY customer_id_sum_abs_balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS seq_no_avg_3
    FROM scored
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.opened_date,
    s.account_type_status,
    s.abs_balance,
    s.running_count_balance,
    s.running_avg_balance,
    (SELECT AVG(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM enriched s;
