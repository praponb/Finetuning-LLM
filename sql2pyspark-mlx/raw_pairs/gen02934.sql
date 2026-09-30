SELECT
    discount,
    order_date,
    store_id,
    INITCAP(TRIM(status)) AS status_title
FROM orders
WHERE status = 'CANCELLED' AND store_id = 16
