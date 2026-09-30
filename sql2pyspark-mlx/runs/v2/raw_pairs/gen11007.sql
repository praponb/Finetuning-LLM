SELECT DISTINCT status, method FROM payments WHERE status IN ('REFUNDED', 'SUCCESS') ORDER BY status
