WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        status,
        CONCAT(status, '/', carrier) status_carrier,
        COALESCE(carrier, 'UNKNOWN') carrier_filled
    FROM shipments
    WHERE order_id IS NOT NULL
      AND carrier NOT IN ('FedEx', 'DHL', 'Flash')
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.shipped_date,
        src.status,
        src.status_carrier,
        o.order_date,
        o.shipping_fee,
        o.channel
    FROM valid src
    INNER JOIN orders o
        ON src.order_id = o.id
    WHERE o.customer_id IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipping_fee DESC, id ASC) AS latest_rank
    FROM joined
    QUALIFY latest_rank = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.shipped_date,
        a.status,
        p.discount
    FROM deduped a
    LEFT JOIN orders p
        ON a.order_id <=> p.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.status AS order_status,
        p.discount AS order_discount
    FROM with_dims s
    LEFT JOIN orders p
        ON s.order_id = p.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    order_id,
    measure,
    value
FROM (
    SELECT
        id,
        order_id,
        discount,
        order_discount
    FROM enriched
) src
UNPIVOT (
    value FOR measure IN (discount, order_discount)
)
