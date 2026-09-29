SELECT id, shipping_fee, store_id, status, CASE WHEN status IN ('RETURNED', 'PENDING') THEN 'Group A' ELSE 'Other' END AS status_group FROM orders WHERE amount > 700 AND status = 'CANCELLED';
