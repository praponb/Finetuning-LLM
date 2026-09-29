WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(unit_price, 0) unit_price_filled
    FROM order_items
    WHERE order_id IN (3, 14, 39)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY quantity DESC, id) event_seq,
        LEAD(unit_price_filled, 1) OVER (PARTITION BY order_id ORDER BY unit_price, id) next_unit_price_filled
    FROM valid
    WHERE unit_price > 330
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_filled,
        event_seq,
        next_unit_price_filled,
        ROUND(event_seq * 0.9, 2) event_seq_scaled
    FROM scored
    WHERE product_id IS NOT NULL
      AND product_id = 33
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        event_seq,
        next_unit_price_filled,
        event_seq_scaled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rn
        FROM base
    ) x
    WHERE rn = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC NULLS FIRST, id) = 1
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        order_id,
        product_id,
        PERCENTILE_APPROX(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
        ROUND(AVG(event_seq), 2) AS avg_event_seq,
        ROUND(AVG(unit_price), 2) AS avg_unit_price,
        GROUPING_ID() AS grouping_level
    FROM latest
    GROUP BY CUBE (order_id, product_id)
    HAVING ROUND(AVG(unit_price), 2) < 1000000
)
SELECT
    s.order_id,
    s.event_seq_quartiles,
    s.avg_event_seq,
    s.avg_unit_price,
    s.grouping_level,
    (SELECT MIN(o2.quantity) FROM order_items o2 WHERE o2.order_id = s.order_id) AS min_order_item_quantity
FROM grouped s
ORDER BY avg_event_seq, order_id
