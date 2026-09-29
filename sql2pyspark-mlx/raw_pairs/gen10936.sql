SELECT MONTH(paid_at) AS paid_month, COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END) AS bank_transfer_count FROM payments GROUP BY paid_month;
