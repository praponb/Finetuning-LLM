WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(status, 'Unknown') AS status_filled,
        COALESCE(status, 'DIRECT') AS resolved_status
    FROM accounts
    WHERE status <> 'OPEN'
)
-- QUALIFY-style window filter
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC NULLS LAST, id) AS row_num
    FROM filtered
) x
WHERE row_num = 1;
