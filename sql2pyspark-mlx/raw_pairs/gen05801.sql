WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_SUB(shipped_date, 33) shipped_date_minus_33d
    FROM shipments
    WHERE delivered_date > DATE '2023-06-01'
),
with_dims AS (
    -- Attach orders attributes
    SELECT s.*, p.shipping_fee
    FROM filtered s
    INNER JOIN orders p
        ON s.order_id = p.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, carrier ORDER BY shipping_fee DESC, id) AS rn
    FROM with_dims
    QUALIFY rn = 1
),
ranked_shipments AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        shipped_date,
        cost,
        status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, carrier ORDER BY rn DESC, id) AS rank_in_group
        FROM ranked
    ) t
    WHERE rank_in_group = 1
)
SELECT id, status
FROM ranked_shipments
ORDER BY id NULLS FIRST, status
