WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        opened_date,
        status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance, id) AS rn
        FROM accounts
    ) ranked
    WHERE rn = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    status,
    COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM ranked
GROUP BY 1, 2
