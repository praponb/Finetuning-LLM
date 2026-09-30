SELECT
    order_id,
    cost,
    ROUND(cost, 1) AS cost_rounded,
    COALESCE(cost, 0) AS cost_filled
FROM shipments
