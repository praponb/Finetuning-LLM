SELECT SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count, SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded_count FROM payments;
