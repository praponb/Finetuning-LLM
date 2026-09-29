WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        LOWER(account_type) AS account_type_lower
    FROM accounts
    WHERE balance < 33000
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY opened_date NULLS FIRST, id) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT src.*, b.signup_date
    FROM not_flagged src
    LEFT JOIN customers b
        ON src.customer_id = b.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    COUNT_IF(status = 'CLOSED') AS closed_events,
    COUNT_IF(status = 'FROZEN') AS frozen_events,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT_IF(status = 'OPEN') AS open_events
FROM enriched
GROUP BY DATE_TRUNC('YEAR', signup_date), DATE_TRUNC('QUARTER', opened_date);
