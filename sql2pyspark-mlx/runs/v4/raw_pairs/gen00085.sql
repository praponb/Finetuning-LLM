WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        FLOOR(amount) AS amount_whole
    FROM payments
    WHERE order_id IS NOT NULL
      AND order_id IS NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY RANK() OVER (PARTITION BY order_id, method ORDER BY paid_at NULLS FIRST) = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.discount,
        b.shipping_fee
    FROM ranked e
    LEFT JOIN orders b
        ON e.order_id = b.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        method,
        shipping_fee,
        discount
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (shipping_fee, discount)
);
