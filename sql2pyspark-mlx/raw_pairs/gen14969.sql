WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CONCAT_WS('-', txn_type, merchant) AS txn_type_merchant_key
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant IS NOT DISTINCT FROM '7-Eleven'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN merchant IN ('Lazada', 'Amazon') THEN 'Core' ELSE 'Other' END AS merchant_group
    FROM cleaned
    WHERE amount <= 1600
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) AS seq_no,
        LAST_VALUE(amount) IGNORE NULLS OVER w_part AS last_known_amount,
        SUM(amount) OVER w_grp AS cumulative_amount
    FROM filtered
    WINDOW w_part AS (PARTITION BY DATE_TRUNC('week', txn_time), merchant ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY account_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        merchant,
        is_flagged,
        merchant_group,
        seq_no,
        last_known_amount,
        LEAD(last_known_amount) OVER (PARTITION BY merchant ORDER BY amount ASC, id ASC) AS next_last_known_amount,
        LAG(amount) OVER (PARTITION BY merchant ORDER BY amount ASC, id ASC) AS prev_amount,
        LEAD(amount) OVER (PARTITION BY merchant ORDER BY amount ASC, id ASC) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount ASC, id ASC) AS event_seq
    FROM metrics
    WHERE is_flagged
),
cleaned_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN next_amount >= 4000 THEN 'High' WHEN next_amount >= 1200 THEN 'Medium' ELSE 'Low' END AS next_amount_tier,
        LENGTH(merchant) AS merchant_length
    FROM enriched
    WHERE merchant IS NOT NULL
      AND last_known_amount > 3800
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.id,
        e.account_id,
        e.seq_no,
        e.last_known_amount,
        e.next_last_known_amount,
        e.next_amount,
        e.event_seq,
        e.next_amount_tier,
        e.merchant_length,
        b.status
    FROM cleaned_transactions e
    LEFT JOIN accounts b
        ON e.account_id = b.id
)
-- Ranking and gap features
SELECT
    id,
    account_id,
    last_known_amount,
    next_last_known_amount,
    next_amount,
    merchant_length,
    status,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY next_amount DESC) AS next_amount_percent_rank,
    SUM(last_known_amount) OVER (PARTITION BY account_id ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last_known_amount
FROM with_dims
