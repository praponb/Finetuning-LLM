SELECT *, GREATEST(amount, shipping_fee) greatest_amount_shipping_fee, amount - shipping_fee amount_with_shipping
FROM orders
WHERE shipping_fee >= 31 AND customer_id IS NOT NULL AND order_date BETWEEN '2024-07-16' AND '2025-09-01'
