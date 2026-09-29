SELECT YEAR(paid_at) AS paid_year, COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count FROM payments WHERE status = 'SUCCESS' GROUP BY YEAR(paid_at);
