SELECT status, SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded_count FROM payments GROUP BY 1;
