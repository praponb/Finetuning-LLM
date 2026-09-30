SELECT status, ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee FROM orders WHERE status = 'COMPLETED' AND channel IN ('store', 'partner') AND status <> 'RETURNED' GROUP BY status
