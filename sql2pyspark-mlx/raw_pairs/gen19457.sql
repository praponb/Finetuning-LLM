WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        DATE_TRUNC('WEEK', opened_date) AS opened_date_period
    FROM accounts
    WHERE opened_date > DATE '2023-09-26'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.merchant IS NOT NULL
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        account_type,
        opened_date,
        status
    FROM kept
    QUALIFY RANK() OVER (PARTITION BY account_type ORDER BY opened_date) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY opened_date DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY opened_date DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date, id) AS row_seq_2,
        ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS row_seq_3
    FROM deduped
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND NOT x.is_flagged
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq_3 >= 9 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_3_tier,
    COUNT(*) AS event_count,
    COUNT_IF(account_type = 'CREDIT') AS credit_events,
    ROUND(SUM(row_seq_3) / NULLIF(COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END), 0), 2) AS row_seq_3_per_savings
FROM not_flagged
GROUP BY CASE WHEN row_seq_3 >= 9 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY row_seq_3_per_savings NULLS FIRST
