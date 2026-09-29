WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY unit_price) AS unit_price_dense_rank,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) AS quantity_share,
        LAG(quantity, 2, 0) OVER (ORDER BY quantity, id) AS prev_quantity
    FROM order_items
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.quantity_share,
    s.prev_quantity,
    (SELECT MIN(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS min_shipment_cost
FROM scored s;
