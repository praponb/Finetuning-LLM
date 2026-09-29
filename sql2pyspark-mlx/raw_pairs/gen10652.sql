WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        AVG(amount) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5,
        CUME_DIST() OVER (PARTITION BY YEAR(txn_time), merchant ORDER BY amount DESC) AS amount_cume_dist
    FROM transactions
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        amount_cume_dist,
        MAX(amount_avg_5) OVER w_grp AS running_max_amount_avg_5,
        SUM(amount_avg_5) OVER w_grp AS running_sum_amount_avg_5,
        COUNT(amount_avg_5) OVER w_grp AS running_count_amount_avg_5,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount ASC, id ASC) AS seq_no
    FROM metrics
    WINDOW w_grp AS (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.amount,
        s.merchant,
        s.amount_cume_dist,
        s.running_max_amount_avg_5,
        s.running_sum_amount_avg_5,
        b.balance
    FROM enriched s
    LEFT JOIN accounts b
        ON s.account_id = b.id
    WHERE b.customer_id = 30
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        merchant,
        amount_cume_dist,
        NTILE(5) OVER (PARTITION BY account_id ORDER BY running_max_amount_avg_5 DESC, id ASC) AS running_max_amount_avg_5_bucket,
        NTILE(3) OVER (PARTITION BY merchant, account_id ORDER BY running_max_amount_avg_5 ASC, id ASC) AS running_max_amount_avg_5_bucket_2,
        LAST_VALUE(running_sum_amount_avg_5) IGNORE NULLS OVER (PARTITION BY merchant ORDER BY running_max_amount_avg_5 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_sum_amount_avg_5,
        LEAD(balance, 2) OVER (PARTITION BY merchant ORDER BY running_max_amount_avg_5 ASC, id ASC) AS next_balance
    FROM joined
)
SELECT
    s.id,
    s.account_id,
    s.merchant,
    s.amount_cume_dist,
    s.running_max_amount_avg_5_bucket,
    s.last_known_running_sum_amount_avg_5,
    s.next_balance,
    CASE WHEN merchant RLIKE '^(7-Eleven|Grab)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS merchant_zone,
    (SELECT AVG(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS avg_transaction_amount
FROM scored s
ORDER BY merchant_zone ASC;
