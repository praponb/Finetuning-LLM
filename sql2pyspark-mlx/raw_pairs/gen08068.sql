WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN txn_type IN ('DEBIT', 'CREDIT') THEN 'Core' ELSE 'Other' END txn_type_group,
        QUARTER(txn_time) txn_time_quarter,
        COALESCE(merchant, 'DIRECT') resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_time >= DATE '2023-06-12'
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.amount,
        a.txn_type_group,
        a.txn_time_quarter,
        b.status
    FROM filtered a
    INNER JOIN accounts b
        ON a.account_id = b.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time_quarter DESC, id) AS row_num
    FROM with_dims
    QUALIFY row_num = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        amount,
        MIN(amount) OVER (PARTITION BY account_id, status) account_id_min_amount,
        LAG(amount, 1) OVER (PARTITION BY status, account_id ORDER BY row_num, id) prev_amount,
        ROW_NUMBER() OVER (ORDER BY row_num DESC, id) event_seq
    FROM ranked
    WHERE account_id IN (1, 12, 27)
),
enriched_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(prev_amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_prev_amount,
        AVG(prev_amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_prev_amount,
        COUNT(prev_amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_count_prev_amount,
        RANK() OVER (ORDER BY account_id_min_amount) account_id_min_amount_rank
    FROM enriched
)
-- Multi-level aggregation
SELECT
    CASE WHEN account_id_min_amount >= 3938 THEN 'HIGH' ELSE 'LOW' END AS account_id_min_amount_tier,
    COUNT(*) AS event_count,
    COUNT(DISTINCT account_id) AS distinct_account_ids
FROM enriched_transactions
GROUP BY CASE WHEN account_id_min_amount >= 3938 THEN 'HIGH' ELSE 'LOW' END;
