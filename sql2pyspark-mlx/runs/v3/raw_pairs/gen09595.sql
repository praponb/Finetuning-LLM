SELECT *, UPPER(status) AS status_upper
FROM payments
WHERE status = 'REFUNDED' AND status IN ('FAILED', 'REFUNDED') AND amount >= 1210
ORDER BY order_id DESC, paid_at;
