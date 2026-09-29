WITH with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.status
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.status
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    status,
    CASE WHEN store_name RLIKE '^(Downtown Express|Old Town)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS store_name_zone
FROM with_latest
