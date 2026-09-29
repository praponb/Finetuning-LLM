WITH valid AS (
    SELECT
        *,
        LENGTH(status) status_length,
        COALESCE(amount, 0) amount_filled,
        COALESCE(method, 'OTHER') resolved_method
    FROM payments
    WHERE order_id IS NOT NULL
      AND method IN ('bank_transfer', 'card')
),
enriched AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        status_length,
        amount_filled,
        LAG(status_length, 1) OVER (PARTITION BY TO_DATE(paid_at) ORDER BY amount_filled, id) prev_status_length,
        SUM(amount_filled) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_amount_filled,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount_filled, id) row_seq
    FROM valid
),
enriched_payments AS (
    SELECT
        s.id,
        s.order_id,
        s.amount,
        s.status,
        s.running_amount_filled,
        p.amount AS order_amount,
        p.shipping_fee,
        p.status AS order_status
    FROM enriched s
    LEFT JOIN orders p
        ON s.order_id = p.id
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY order_status ORDER BY shipping_fee DESC NULLS LAST) AS dedup_rank
    FROM enriched_payments
    QUALIFY dedup_rank = 1
)
SELECT
    s.id,
    s.order_id,
    s.amount,
    s.running_amount_filled,
    s.order_amount,
    s.dedup_rank,
    (SELECT SUM(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS sum_shipment_cost
FROM latest s
ORDER BY running_amount_filled DESC NULLS LAST, order_id DESC
