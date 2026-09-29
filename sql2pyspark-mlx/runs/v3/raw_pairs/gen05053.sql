SELECT TO_DATE(paid_at) AS paid_day, MAX(paid_at) AS latest_paid_at FROM payments WHERE paid_at >= '2024-07-01' GROUP BY paid_day ORDER BY latest_paid_at, paid_day LIMIT 5
