SELECT paid_at, CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END AS is_refunded, CEIL(amount) AS amount_whole
FROM payments
WHERE (status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') AND amount BETWEEN 720 AND 1500) OR paid_at >= '2024-12-27'
ORDER BY paid_at, is_refunded DESC
