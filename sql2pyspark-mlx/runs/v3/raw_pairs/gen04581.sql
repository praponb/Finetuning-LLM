SELECT QUARTER(paid_at) AS paid_quarter, status, MIN(amount) AS min_amount FROM payments WHERE method <> 'promptpay' AND status = 'FAILED' GROUP BY QUARTER(paid_at), status
