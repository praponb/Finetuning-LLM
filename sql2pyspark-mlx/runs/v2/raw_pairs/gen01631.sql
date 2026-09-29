SELECT
    id,
    customer_id,
    store_id,
    shipping_fee,
    INITCAP(TRIM(status)) AS status_title,
    CASE WHEN status IN ('COMPLETED', 'SHIPPED') THEN 'Tier 1' ELSE 'Other' END AS status_group
FROM orders
WHERE discount < 0.12 AND status IN ('RETURNED', 'PENDING', 'COMPLETED')
