SELECT COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) refunded_count FROM payments ORDER BY refunded_count DESC
