WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS latest_rank
        FROM payments
    ) x
    WHERE latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    order_id,
    ROUND(AVG(amount), 2) AS avg_amount,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    MAX(amount) FILTER (WHERE method = 'promptpay') AS promptpay_max_amount
FROM deduped
GROUP BY order_id
HAVING promptpay_max_amount < 1000000
ORDER BY promptpay_max_amount DESC NULLS LAST;
