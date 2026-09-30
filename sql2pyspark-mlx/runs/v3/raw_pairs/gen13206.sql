SELECT *, DATE_FORMAT(paid_at, 'MMM yyyy') AS paid_at_label
FROM payments
WHERE paid_at < '2023-03-01' AND TO_DATE(paid_at) >= '2023-12-01'
ORDER BY order_id DESC, id
LIMIT 1;
