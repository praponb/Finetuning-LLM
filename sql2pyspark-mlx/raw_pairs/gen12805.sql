WITH filtered AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        UPPER(carrier) AS carrier_upper,
        CASE WHEN carrier = 'DHL' THEN 1 ELSE 0 END AS is_dhl,
        COALESCE(carrier, 'DIRECT') AS resolved_carrier
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-09-23')
),
ranked AS (
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        carrier_upper,
        resolved_carrier
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS rank_in_group
        FROM filtered
    ) t
    WHERE rank_in_group = 1
),
enriched AS (
    SELECT e.*, b.status AS order_status
    FROM ranked e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
cleaned AS (
    SELECT
        *,
        UPPER(order_status) AS order_status_upper,
        COALESCE(order_status, 'UNKNOWN') AS order_status_filled
    FROM enriched
    WHERE delivered_date >= DATE '2023-05-26'
      AND order_id IS NULL
)
SELECT
    s.id,
    s.order_id,
    s.cost,
    s.status,
    s.carrier_upper,
    s.resolved_carrier,
    s.order_status,
    CASE WHEN order_status RLIKE '^(PENDING|SHIPPED)$' THEN 'GROUP_A' WHEN order_status RLIKE '^(COMPLETED|CANCELLED)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS order_status_zone,
    (SELECT MAX(x.quantity) FROM order_items x WHERE x.order_id = s.order_id) AS max_order_item_quantity
FROM cleaned s
ORDER BY id DESC NULLS LAST, order_status
