SELECT
    s.cost,
    s.shipped_date,
    o.amount,
    DATE_TRUNC('WEEK', o.order_date) AS order_date_period
FROM shipments s
FULL OUTER JOIN orders o ON o.id = s.order_id
ORDER BY amount;
