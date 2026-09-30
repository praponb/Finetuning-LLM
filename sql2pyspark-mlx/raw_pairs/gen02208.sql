WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN amount >= 4000 THEN 'High' WHEN amount >= 1800 THEN 'Medium' ELSE 'Low' END AS amount_band,
        COALESCE(merchant, 'N/A') AS resolved_merchant
    FROM transactions
    WHERE txn_time > TO_TIMESTAMP('2023-10-11 19:00:00')
      AND merchant IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(amount) OVER (PARTITION BY txn_type ORDER BY amount, id) AS next_amount,
        LAG(amount) OVER (PARTITION BY txn_type ORDER BY amount, id) AS prev_amount,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount, id) AS event_seq,
        AVG(amount) OVER (PARTITION BY account_id) AS account_id_avg_amount
    FROM valid
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY event_seq DESC, id) = 1
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.resolved_merchant,
        s.event_seq,
        s.account_id_avg_amount,
        b.balance,
        b.opened_date,
        b.status
    FROM ranked s
    LEFT JOIN accounts b
        ON s.account_id = b.id
    WHERE b.customer_id IN (4, 12, 36, 37)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(account_id_avg_amount) OVER (PARTITION BY account_id, status ORDER BY account_id_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_account_id_avg_amount,
        MIN(account_id_avg_amount) OVER (PARTITION BY account_id, status ORDER BY account_id_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_account_id_avg_amount,
        AVG(account_id_avg_amount) OVER (PARTITION BY account_id, status ORDER BY account_id_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_account_id_avg_amount
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    status,
    measure,
    val
FROM (
    SELECT
        id,
        status,
        balance,
        running_avg_account_id_avg_amount
    FROM with_windows
) src
UNPIVOT (
    val FOR measure IN (balance AS BALANCE_VAL, running_avg_account_id_avg_amount AS RUNNING_AVG_ACCOUNT_ID_AVG_AMOUNT)
) unp
