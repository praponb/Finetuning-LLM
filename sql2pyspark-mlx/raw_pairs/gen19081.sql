SELECT
    id,
    cost,
    shipped_date,
    order_id,
    ROUND(cost * 0.9, 2) adjusted_cost
FROM shipments
WHERE shipped_date BETWEEN '2025-07-01' AND '2025-10-11'
ORDER BY order_id DESC, id DESC;
