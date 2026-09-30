WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time
    FROM transactions
    WHERE amount <= 3800
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY amount DESC, id) AS dedup_rank
    FROM cleaned
) x
WHERE dedup_rank = 1
