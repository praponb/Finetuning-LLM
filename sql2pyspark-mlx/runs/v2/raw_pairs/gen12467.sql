SELECT DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, SUM(amount) AS total_amount FROM payments WHERE status = 'REFUNDED' GROUP BY DATE_FORMAT(paid_at, 'yyyy-MM');
