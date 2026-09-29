WITH enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.discount,
        b.order_date,
        b.channel
    FROM shipments src
    JOIN orders b
        ON src.order_id = b.id
    WHERE b.shipping_fee > 20
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY delivered_date DESC NULLS LAST, id ASC) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        a.*,
        o.order_date AS order_order_date,
        o.discount AS order_discount,
        o.amount
    FROM deduped a
    JOIN orders o
        ON a.order_id = o.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.status,
        src.discount,
        src.rn,
        d.channel AS order_channel
    FROM enriched_shipments src
    LEFT JOIN orders d
        ON src.order_id = d.id
)
SELECT
    id,
    carrier,
    status
FROM joined
ORDER BY id DESC NULLS FIRST
LIMIT 10;
