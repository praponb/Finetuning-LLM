WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        UPPER(status) AS status_upper,
        COALESCE(method, 'N/A') AS resolved_method
    FROM payments
    WHERE amount BETWEEN 580 AND 820
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.status,
        e.resolved_method,
        d.status AS order_status,
        d.channel
    FROM filtered e
    LEFT JOIN orders d
        ON e.order_id <=> d.id
    WHERE d.amount >= 1570
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        status,
        resolved_method,
        order_status,
        channel,
        LENGTH(channel) AS channel_length,
        INITCAP(TRIM(status)) AS status_title,
        COALESCE(order_status, 'NONE') AS order_status_filled
    FROM joined
    WHERE order_id IS NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(channel) AS channel_upper
    FROM valid
    WHERE order_status IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'web'
),
enriched AS (
    -- Attach orders attributes
    SELECT s.*, d.discount
    FROM cleaned s
    JOIN orders d
        ON s.order_id = d.id
)
SELECT
    s.id,
    s.order_id,
    s.status,
    s.order_status,
    s.channel,
    s.status_title,
    s.order_status_filled,
    s.channel_upper,
    s.discount,
    CASE WHEN channel RLIKE '^(partner|web)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS channel_zone,
    (SELECT MIN(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS min_shipment_cost
FROM enriched s
ORDER BY min_shipment_cost NULLS FIRST;
