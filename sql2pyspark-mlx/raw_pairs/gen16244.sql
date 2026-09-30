SELECT id, method, order_id, paid_at, CASE WHEN status IN ('REFUNDED', 'SUCCESS') THEN 'Group A' ELSE 'Other' END status_group FROM payments WHERE status = 'REFUNDED'
