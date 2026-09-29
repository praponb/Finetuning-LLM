WITH latest_match AS (
    SELECT
        s.id,
        s.store_id,
        s.order_date,
        s.discount,
        latest.quantity
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    store_id,
    DATE_TRUNC('WEEK', order_date) AS week_start,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90
FROM latest_match
GROUP BY store_id, DATE_TRUNC('WEEK', order_date)
ORDER BY distinct_ids DESC;
