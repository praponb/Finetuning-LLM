WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_count_7d,
        SUM(amount) OVER (PARTITION BY MONTH(txn_time) ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM transactions
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        amount_count_7d,
        LEAD(cumulative_amount) OVER (PARTITION BY merchant, account_id ORDER BY amount, id) AS next_cumulative_amount,
        LAG(cumulative_amount) OVER (PARTITION BY merchant, account_id ORDER BY amount, id) AS prev_cumulative_amount,
        LEAD(amount_count_7d) OVER (PARTITION BY merchant, account_id ORDER BY amount, id) AS next_amount_count_7d,
        ROW_NUMBER() OVER (PARTITION BY merchant, account_id ORDER BY amount, id) AS event_seq,
        SUM(amount_count_7d) OVER (PARTITION BY merchant ORDER BY cumulative_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_count_7d
    FROM metrics
),
remaining_keys AS (
    -- Set difference on keys
    SELECT account_id, id
    FROM scored
    EXCEPT
    SELECT account_id, id
    FROM transactions
    WHERE merchant <> 'Grab'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.account_id = k.account_id AND s.id = k.id
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        p.account_type,
        p.balance
    FROM reconciled e
    LEFT JOIN accounts p
        ON e.account_id = p.id
)
-- Join a dimension and aggregate
SELECT
    p.account_type AS account_account_type,
    p.status,
    COUNT(*) AS num_rows,
    AVG(e.amount_count_7d) AS avg_amount_count_7d
FROM enriched e
LEFT JOIN accounts p ON e.account_id = p.id
GROUP BY ROLLUP (p.account_type, p.status)
ORDER BY avg_amount_count_7d DESC NULLS LAST
