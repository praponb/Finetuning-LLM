WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        status,
        channel,
        amount,
        discount,
        GREATEST(shipping_fee, discount) AS greatest_shipping_fee_discount
    FROM orders
    WHERE order_date >= DATE '2023-09-02'
      AND MONTH(order_date) IN (1, 2, 9)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
joined AS (
    -- Attach stores attributes
    SELECT s.*, p.city
    FROM deduped s
    LEFT JOIN stores p
        ON s.store_id = p.id
    WHERE p.city <> 'Chiang Mai'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT channel, status
    FROM joined
    INTERSECT ALL
    SELECT channel, status
    FROM orders
    WHERE status IN ('PENDING', 'RETURNED', 'CANCELLED')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.channel = k.channel AND s.status = k.status
)
-- Aggregate with conditional metrics
SELECT
    channel,
    CASE WHEN amount >= 1430 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    COUNT_IF(status = 'COMPLETED') AS completed_events,
    COUNT_IF(status = 'COMPLETED') AS completed_events_2,
    AVG(amount) FILTER (WHERE city = 'Tokyo') AS tokyo_avg_amount,
    COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count,
    SUM(CASE WHEN channel = 'web' THEN amount ELSE 0 END) AS web_amount,
    PERCENT_RANK() OVER (PARTITION BY channel ORDER BY COUNT_IF(status = 'COMPLETED') DESC) AS completed_events_2_percent_rank
FROM verified
GROUP BY channel, amount_tier;
