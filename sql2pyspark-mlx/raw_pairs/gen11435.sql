WITH base AS (
    SELECT
        id,
        order_id,
        amount,
        COALESCE(method, 'N/A') AS method_filled,
        COALESCE(method, 'DIRECT') AS method_clean
    FROM payments
    WHERE method IS NOT NULL
      AND paid_at >= '2023-09-01'
),
base_payments AS (
    SELECT
        *,
        CASE WHEN amount >= 1690 THEN 'High' WHEN amount >= 250 THEN 'Medium' ELSE 'Low' END AS amount_level,
        ROUND(amount, 2) AS amount_rounded
    FROM base
    WHERE order_id IS NOT NULL
      AND order_id = 12
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS LAST, id) AS rn
    FROM base_payments
    QUALIFY rn = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount_rounded DESC, id) AS dedup_rank
        FROM deduped
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    s.id,
    s.order_id,
    s.amount,
    s.amount_level,
    s.rn,
    s.dedup_rank,
    (SELECT AVG(t2.quantity) FROM order_items t2 WHERE t2.order_id = s.order_id) AS avg_order_item_quantity
FROM ranked s
ORDER BY avg_order_item_quantity DESC NULLS LAST, dedup_rank DESC
