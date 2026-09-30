WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ROUND(unit_price, 0) AS unit_price_rounded,
        CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price_rounded, id) = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        order_id,
        CASE WHEN quantity >= 7 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
        ROUND(AVG(unit_price_rounded), 2) AS avg_unit_price_rounded,
        AVG(unit_price) AS avg_unit_price
    FROM deduped
    GROUP BY order_id, CASE WHEN quantity >= 7 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    s.order_id,
    s.quantity_tier,
    s.avg_unit_price_rounded,
    s.avg_unit_price,
    (SELECT MAX(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS max_shipment_cost
FROM summary s
ORDER BY avg_unit_price DESC
