WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND TO_DATE(c.txn_time) < '2024-05-04'
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT id, status
    FROM not_flagged
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY opened_date NULLS LAST) = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(status, 'unassigned') AS status_filled,
        COALESCE(status, 'DIRECT') AS resolved_status
    FROM deduped
    WHERE status NOT IN ('FROZEN', 'OPEN')
),
not_flagged_accounts AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.is_flagged = TRUE
        )
)
-- Grouped metrics
SELECT
    status,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count
FROM not_flagged_accounts
GROUP BY status
ORDER BY num_status NULLS FIRST;
