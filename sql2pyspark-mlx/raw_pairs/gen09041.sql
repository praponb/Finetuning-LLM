SELECT method, MAX(paid_at) last_paid_at FROM payments WHERE paid_at >= '2024-12-31' AND method IN ('card', 'cash') AND amount <= 560 GROUP BY method;
