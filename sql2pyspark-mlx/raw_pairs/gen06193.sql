WITH filtered_transactions AS (
    SELECT *
    FROM transactions
    WHERE account_id IN (7, 13)
),
summary AS (
    SELECT
        CASE WHEN merchant IN ('Lazada', 'Shell') THEN 'priority' ELSE 'other' END AS merchant_group,
        CASE WHEN amount >= 3400 THEN 'high' WHEN amount >= 1000 THEN 'mid' ELSE 'low' END AS amount_bucket,
        SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
        MAX(amount) AS max_amount
    FROM filtered_transactions
    GROUP BY CASE WHEN merchant IN ('Lazada', 'Shell') THEN 'priority' ELSE 'other' END, CASE WHEN amount >= 3400 THEN 'high' WHEN amount >= 1000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
WHERE max_amount > 2200
ORDER BY max_amount DESC
