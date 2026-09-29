WITH valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        DAY(txn_time) AS txn_time_day,
        UPPER(merchant) AS merchant_upper
    FROM transactions
    WHERE merchant IS NOT DISTINCT FROM 'Lazada'
      AND merchant IS NOT NULL
),
joined AS (
    SELECT
        s.*,
        b.balance,
        b.status,
        b.account_type
    FROM valid s
    LEFT JOIN accounts b
        ON s.account_id = b.id
),
with_windows AS (
    SELECT
        id,
        account_id,
        txn_type,
        is_flagged,
        txn_time_day,
        merchant_upper,
        balance,
        status,
        account_type,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY txn_time DESC, id) AS event_seq,
        LEAD(balance, 1, 0) OVER (PARTITION BY account_id ORDER BY txn_time_day, id) AS next_balance
    FROM joined
),
metrics AS (
    SELECT
        *,
        SUM(event_seq) OVER (PARTITION BY status, account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_event_seq,
        MAX(event_seq) OVER (PARTITION BY status, account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_event_seq,
        AVG(event_seq) OVER (PARTITION BY status, account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_event_seq,
        SUM(next_balance) OVER (PARTITION BY account_id ORDER BY txn_time_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_balance
    FROM with_windows
    WHERE account_id IN (24, 34)
),
with_dims AS (
    SELECT
        a.id,
        a.account_id,
        a.txn_type,
        a.is_flagged,
        a.txn_time_day,
        a.merchant_upper,
        a.account_type,
        a.event_seq,
        a.next_balance,
        a.running_sum_event_seq,
        a.running_max_event_seq,
        a.running_avg_event_seq,
        d.balance AS account_balance
    FROM metrics a
    INNER JOIN accounts d
        ON a.account_id = d.id
),
enriched AS (
    SELECT
        src.id,
        src.account_id,
        src.txn_type,
        src.is_flagged,
        src.txn_time_day,
        src.account_type,
        src.next_balance,
        src.running_max_event_seq,
        src.running_avg_event_seq,
        src.account_balance,
        b.opened_date
    FROM with_dims src
    INNER JOIN accounts b
        ON src.account_id = b.id
)
SELECT *
FROM (
    SELECT
        txn_type,
        account_id,
        account_type,
        running_avg_event_seq
    FROM enriched
) src
PIVOT (
    AVG(running_avg_event_seq) AS amount
    FOR account_type IN ('CREDIT', 'CHECKING')
)
