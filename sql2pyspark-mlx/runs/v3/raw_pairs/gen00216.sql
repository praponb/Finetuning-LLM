SELECT status, amount, DATE_TRUNC('MONTH', paid_at) AS paid_at_period, CONCAT_WS(',', method, status) AS method_status_key FROM payments WHERE amount >= 840 ORDER BY status;
