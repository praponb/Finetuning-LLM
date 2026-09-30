SELECT id, discount, shipping_fee, CASE WHEN status IN ('PENDING', 'CANCELLED') THEN 'Group A' ELSE 'Other' END AS status_group FROM orders WHERE customer_id IS NOT NULL AND YEAR(order_date) >= 2025;
