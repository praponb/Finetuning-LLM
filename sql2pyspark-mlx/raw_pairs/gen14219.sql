SELECT shipping_fee
FROM orders
WHERE amount IS NOT NULL
UNION
SELECT cost
FROM shipments
WHERE shipped_date <= '2025-08-10'
ORDER BY shipping_fee DESC;
