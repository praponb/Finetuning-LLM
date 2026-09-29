WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM accounts
    WHERE status IS DISTINCT FROM 'CLOSED'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        days_since_opened_date,
        CONCAT(account_type, ' - ', status) AS account_type_status
    FROM filtered
    WHERE balance < 57000
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        account_type,
        status,
        account_type_status
    FROM valid
    QUALIFY RANK() OVER (PARTITION BY account_type ORDER BY days_since_opened_date DESC) = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.amount < 2100
        )
)
-- Multi-level aggregation
SELECT
    status,
    COUNT_IF(status = 'CLOSED') AS closed_events,
    COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END) AS loan_count,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
FROM kept
GROUP BY status
HAVING loan_count >= 1
ORDER BY loan_count DESC;
