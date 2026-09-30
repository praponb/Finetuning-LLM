WITH base AS (
    SELECT *
    FROM orders
    WHERE status IN ('SHIPPED', 'PENDING') AND store_id IN (5, 28)
),
summary AS (
    SELECT channel, MAX(amount) AS max_amount
    FROM base
    GROUP BY channel
)
SELECT channel, max_amount
FROM summary
WHERE max_amount < 1240
ORDER BY 2 DESC, channel DESC;
