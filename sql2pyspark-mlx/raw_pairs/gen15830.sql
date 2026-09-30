WITH eligible AS (
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.cost > 92
        )
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.status,
    s.channel,
    s.amount,
    s.discount,
    s.shipping_fee,
    (SELECT MIN(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS min_account_balance
FROM eligible s;
