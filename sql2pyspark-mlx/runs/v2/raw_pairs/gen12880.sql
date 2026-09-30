SELECT
    id,
    store_id,
    discount,
    LOWER(status) AS status_lower
FROM orders
WHERE status = 'CANCELLED'
