WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY amount DESC NULLS LAST, id) AS rn
        FROM transactions
    ) t
    WHERE rn = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, account_id
    FROM deduped
    EXCEPT DISTINCT
    SELECT id, account_id
    FROM transactions
    WHERE is_flagged = TRUE
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN remaining_keys k ON s.id = k.id AND s.account_id = k.account_id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        rn,
        SUBSTRING(merchant, 1, 2) merchant_prefix
    FROM reconciled
    WHERE account_id IN (2, 24, 32, 36)
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.amount,
        a.merchant,
        d.balance,
        d.opened_date
    FROM base a
    LEFT JOIN accounts d
        ON a.account_id = d.id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('YEAR', opened_date) AS year_start,
        DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
        SUM(balance) FILTER (WHERE merchant = 'Starbucks') AS starbucks_total_balance,
        SIZE(COLLECT_SET(merchant)) AS num_merchants,
        ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN merchant = 'Shell' THEN 1 END), 0), 2) AS amount_per_shell,
        COUNT(DISTINCT merchant) AS distinct_merchants,
        ROUND(AVG(balance), 2) AS avg_balance
    FROM with_dims
    GROUP BY DATE_TRUNC('YEAR', opened_date), DATE_TRUNC('QUARTER', opened_date)
)
SELECT
    year_start,
    quarter_start,
    starbucks_total_balance,
    num_merchants,
    distinct_merchants,
    avg_balance
FROM daily;
