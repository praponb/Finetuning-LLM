WITH base AS (
    SELECT *, LOWER(txn_type) AS txn_type_lower
    FROM transactions
    WHERE merchant IS NOT NULL
),
cleaned AS (
    SELECT *, SUBSTRING(txn_type, 1, 4) AS txn_type_prefix
    FROM base
    WHERE merchant IS DISTINCT FROM '7-Eleven'
),
ranked AS (
    SELECT
        id,
        account_id,
        amount,
        merchant,
        is_flagged,
        txn_type_lower
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type, account_id ORDER BY amount, id) = 1
),
scored AS (
    SELECT
        *,
        LAG(amount) OVER (PARTITION BY account_id ORDER BY amount, id) AS prev_amount,
        LEAD(amount) OVER (PARTITION BY account_id ORDER BY amount, id) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount, id) AS row_seq,
        AVG(amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5
    FROM ranked
    WHERE account_id IN (8, 23, 31, 40)
),
matched_keys AS (
    SELECT id
    FROM scored
    EXCEPT DISTINCT
    SELECT id
    FROM transactions
    WHERE amount BETWEEN 1900 AND 3400
),
kept_rows AS (
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.id = k.id
),
enriched AS (
    SELECT
        a.*,
        p.status,
        p.account_type
    FROM kept_rows a
    LEFT JOIN accounts p
        ON a.account_id = p.id
)
SELECT
    account_id,
    COUNT(DISTINCT id) AS distinct_ids,
    STDDEV_SAMP(amount) AS amount_stddev,
    COUNT_IF(account_type = 'SAVINGS') AS savings_events,
    SIZE(COLLECT_SET(status)) AS num_status
FROM enriched
GROUP BY account_id
ORDER BY amount_stddev;
