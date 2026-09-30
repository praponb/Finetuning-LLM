WITH remaining_keys AS (
    -- Set difference on keys
    SELECT carrier, order_id
    FROM shipments
    EXCEPT
    SELECT carrier, order_id
    FROM shipments
    WHERE cost > 91
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    JOIN remaining_keys k ON s.carrier = k.carrier AND s.order_id = k.order_id
)
SELECT
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.status,
    (SELECT SUM(t2.amount) FROM payments t2 WHERE t2.order_id = s.order_id) AS sum_payment_amount
FROM reconciled s
