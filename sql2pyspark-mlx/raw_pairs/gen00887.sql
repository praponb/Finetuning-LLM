WITH enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.carrier,
        e.shipped_date,
        e.delivered_date,
        e.cost,
        p.order_date,
        p.shipping_fee,
        p.amount
    FROM shipments e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY order_id, carrier ORDER BY shipped_date) = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.shipped_date,
        s.cost,
        b.shipping_fee AS order_shipping_fee,
        b.amount AS order_amount
    FROM deduped s
    INNER JOIN orders b
        ON s.order_id = b.id
    WHERE b.order_date BETWEEN '2024-12-25' AND '2025-10-01'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id) AS dedup_rank
    FROM with_dims
    QUALIFY dedup_rank = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.shipped_date,
        e.cost,
        e.order_amount,
        e.dedup_rank,
        d.shipping_fee,
        d.discount,
        d.order_date
    FROM latest e
    LEFT JOIN orders d
        ON e.order_id = d.id
)
SELECT
    id,
    shipped_date,
    order_amount,
    shipping_fee,
    discount
FROM joined
ORDER BY id DESC, discount
LIMIT 5;
