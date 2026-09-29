WITH ranked_shipments AS (
    SELECT
        id,
        order_id,
        cost,
        delivered_date,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id) AS rnk
    FROM shipments
)
SELECT
    id,
    order_id,
    cost,
    delivered_date
FROM ranked_shipments
WHERE rnk <= 3
ORDER BY delivered_date;
