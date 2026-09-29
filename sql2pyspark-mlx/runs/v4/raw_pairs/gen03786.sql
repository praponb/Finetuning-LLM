WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        status,
        DATE_ADD(shipped_date, 40) AS shipped_date_plus_40d,
        LOWER(status) AS status_lower
    FROM shipments
    WHERE shipped_date >= DATE '2023-04-15'
      AND carrier <> 'FedEx'
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.shipping_fee,
        b.amount,
        b.order_date
    FROM valid e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
joined_shipments AS (
    -- Attach orders attributes
    SELECT
        s.*,
        b.shipping_fee AS order_shipping_fee,
        b.order_date AS order_order_date,
        b.discount
    FROM joined s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.shipping_fee AS order_shipping_fee_2,
        d.order_date AS order_order_date_2
    FROM joined_shipments a
    LEFT JOIN orders d
        ON a.order_id = d.id
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        s.*,
        d.shipping_fee AS order_shipping_fee_3,
        d.order_date AS order_order_date_3,
        d.discount AS order_discount
    FROM enriched s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched_shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY order_order_date DESC NULLS FIRST, id) = 1
)
SELECT
    id,
    carrier,
    shipped_date,
    status,
    status_lower,
    shipping_fee,
    amount,
    order_date,
    order_shipping_fee,
    CASE WHEN carrier RLIKE '^(Flash|FedEx)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS carrier_zone
FROM ranked
ORDER BY shipping_fee DESC;
