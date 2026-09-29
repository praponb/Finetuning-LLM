SELECT id, shipping_fee, shipping_fee / NULLIF(discount, 0) AS shipping_fee_per_discount FROM orders WHERE amount <= 410 AND order_date <= '2025-01-01';
