WITH joined AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.quantity,
        o.status
    FROM order_items e
    LEFT JOIN orders o
        ON e.order_id = o.id
    WHERE o.customer_id IN (7, 36, 39)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) AS row_num
    FROM joined
    QUALIFY row_num = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, order_id
    FROM deduped
    EXCEPT DISTINCT
    SELECT id, order_id
    FROM order_items
    WHERE order_id IN (25, 34, 37)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.order_id = k.order_id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.status,
        p.shipping_fee,
        p.discount,
        p.amount
    FROM reconciled src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.discount,
        src.amount,
        d.channel
    FROM enriched src
    INNER JOIN orders d
        ON src.order_id = d.id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN discount >= 0.36 THEN 'LARGE' ELSE 'SMALL' END AS discount_tier,
        SUM(CASE WHEN channel = 'partner' THEN amount ELSE 0 END) AS partner_amount,
        COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count
    FROM with_dims
    GROUP BY CASE WHEN discount >= 0.36 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT discount_tier, store_count
FROM daily;
