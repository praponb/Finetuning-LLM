WITH latest AS (
    SELECT
        id,
        order_id,
        paid_at
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount NULLS LAST, id) AS row_num
        FROM payments
    ) t
    WHERE row_num = 1
)
SELECT
    s.id,
    s.order_id,
    (SELECT AVG(t2.quantity) FROM order_items t2 WHERE t2.order_id = s.order_id) AS avg_order_item_quantity
FROM latest s
ORDER BY id
