WITH customer_stats AS (
    SELECT customer_id, SUM(CASE WHEN channel = 'partner' THEN discount ELSE 0 END) AS partner_discount, ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee
    FROM orders
    WHERE customer_id IN (9, 12)
    GROUP BY customer_id
)
SELECT
    c.id,
    c.is_active,
    cs.partner_discount,
    cs.avg_shipping_fee
FROM customers c
INNER JOIN customer_stats cs ON c.id = cs.customer_id
WHERE cs.avg_shipping_fee >= 75
