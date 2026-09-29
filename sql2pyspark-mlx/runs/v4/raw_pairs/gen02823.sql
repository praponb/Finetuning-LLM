WITH valid AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CAST(txn_time AS DATE) AS txn_time_day,
        DATE_TRUNC('WEEK', txn_time) AS txn_time_period,
        COALESCE(merchant, 'NONE') AS merchant_clean
    FROM transactions
    WHERE txn_time >= TO_TIMESTAMP('2024-02-08 07:00:00')
      AND merchant IS NOT NULL
),
scored AS (
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant, account_id), 4) AS amount_share,
        LAG(amount, 1) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_amount,
        STDDEV_SAMP(amount) OVER w_recent AS account_id_stddev_samp_amount,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time_day, id) AS event_seq,
        STDDEV_SAMP(amount) OVER (PARTITION BY merchant) AS merchant_stddev_samp_amount
    FROM valid
    WINDOW w_recent AS (PARTITION BY account_id)
),
metrics AS (
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id, merchant), 4) AS amount_share_2,
        FIRST_VALUE(account_id_stddev_samp_amount) IGNORE NULLS OVER (PARTITION BY merchant, account_id ORDER BY txn_time_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_account_id_stddev_samp_amount,
        AVG(amount) OVER (ORDER BY amount_share, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior
    FROM scored
)
SELECT
    id,
    amount,
    txn_time,
    merchant,
    txn_time_day,
    txn_time_period,
    merchant_clean,
    amount_share,
    account_id_stddev_samp_amount
FROM metrics
