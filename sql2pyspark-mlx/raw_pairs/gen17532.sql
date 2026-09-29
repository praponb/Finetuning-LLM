WITH filtered AS (
    SELECT *, DATE_TRUNC('month', opened_date) AS opened_date_period
    FROM accounts
    WHERE opened_date >= DATE '2023-05-06'
      AND status IS NOT NULL
),
metrics AS (
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY opened_date_period, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY opened_date_period, id) AS event_seq
    FROM filtered
),
eligible AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id = 11
        )
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS event_seq_2,
        NTILE(4) OVER (PARTITION BY account_type ORDER BY event_seq DESC, id) AS event_seq_bucket
    FROM eligible
    WINDOW w_ordered AS (PARTITION BY status ORDER BY balance, id)
),
with_last AS (
    SELECT s.*, top1.transaction_txn_type
    FROM enriched s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date_period
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    SELECT
        id,
        customer_id,
        account_type,
        opened_date_period,
        event_seq,
        event_seq_bucket,
        COUNT(balance) OVER w_recent AS running_count_balance,
        SUM(balance) OVER w_recent AS running_sum_balance,
        MIN(balance) OVER w_recent AS running_min_balance,
        FIRST_VALUE(balance_avg_6_prior) IGNORE NULLS OVER (PARTITION BY status ORDER BY balance_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance_avg_6_prior
    FROM with_last
    WHERE transaction_txn_type IN ('FEE', 'REFUND')
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY balance_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        *,
        MIN(event_seq_bucket) OVER (PARTITION BY customer_id ORDER BY first_known_balance_avg_6_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_event_seq_bucket,
        MAX(event_seq_bucket) OVER (PARTITION BY customer_id ORDER BY first_known_balance_avg_6_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_event_seq_bucket,
        COUNT(event_seq_bucket) OVER (PARTITION BY customer_id ORDER BY first_known_balance_avg_6_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_event_seq_bucket,
        SUM(event_seq_bucket) OVER (PARTITION BY customer_id ORDER BY first_known_balance_avg_6_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_event_seq_bucket
    FROM scored
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.opened_date_period,
    s.event_seq,
    s.running_count_balance,
    s.running_sum_balance,
    s.running_min_balance,
    s.first_known_balance_avg_6_prior,
    CASE WHEN account_type RLIKE '^(SAVINGS|CREDIT)$' THEN 'EXTENDED' WHEN account_type RLIKE '^(CHECKING|LOAN)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS account_type_zone,
    (SELECT MIN(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM with_windows s
ORDER BY id DESC
LIMIT 10
