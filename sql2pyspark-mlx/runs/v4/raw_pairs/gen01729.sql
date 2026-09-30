WITH with_dims AS (
    -- Attach stores attributes
    SELECT
        src.*,
        p.sqft,
        p.region,
        p.store_name
    FROM orders src
    LEFT JOIN stores p
        ON src.store_id = p.id
)
SELECT
    id,
    store_id,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    sqft,
    store_name,
    CASE WHEN status RLIKE '^(SHIPPED|COMPLETED)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS status_zone
FROM with_dims
ORDER BY id DESC NULLS FIRST;
