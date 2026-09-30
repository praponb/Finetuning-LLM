WITH cleaned AS (
    SELECT
        *,
        CASE WHEN status IN ('FAILED', 'REFUNDED') THEN 'Tier 1' ELSE 'Other' END AS status_group,
        COALESCE(method, 'OTHER') AS method_clean
    FROM payments
    WHERE status IN ('FAILED', 'REFUNDED', 'SUCCESS')
      AND order_id IS NOT NULL
),
with_dims AS (
    SELECT
        src.*,
        d.channel,
        d.shipping_fee,
        d.amount AS order_amount
    FROM cleaned src
    LEFT JOIN orders d
        ON src.order_id = d.id
    WHERE d.store_id IS NOT NULL
),
kept_keys AS (
    SELECT method
    FROM with_dims
    EXCEPT DISTINCT
    SELECT method
    FROM payments
    WHERE paid_at < '2025-04-13'
),
reconciled AS (
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.method = k.method
),
with_window AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM reconciled src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.paid_at BETWEEN pr.starts_on AND pr.ends_on
),
scored AS (
    SELECT *, CUME_DIST() OVER w_ordered AS amount_cume_dist
    FROM with_window
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY amount DESC NULLS LAST)
)
SELECT
    s.id,
    s.order_id,
    s.amount,
    s.status,
    s.status_group,
    s.method_clean,
    s.channel,
    s.shipping_fee,
    s.order_amount,
    (SELECT MAX(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS max_shipment_cost
FROM scored s
