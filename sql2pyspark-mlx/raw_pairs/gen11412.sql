WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.island_id,
    CASE WHEN method RLIKE '^(paypal|promptpay)$' THEN 'GROUP_A' WHEN method RLIKE '^(card|bank_transfer)$' THEN 'PRIORITY' ELSE 'CORE' END AS method_zone,
    (SELECT MIN(o2.unit_price) FROM order_items o2 WHERE o2.order_id = s.order_id) AS min_order_item_unit_price
FROM sessions_payments s;
