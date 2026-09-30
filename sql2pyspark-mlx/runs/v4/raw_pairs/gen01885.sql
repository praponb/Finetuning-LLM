SELECT status FROM payments WHERE method IN ('bank_transfer', 'card', 'promptpay') UNION ALL SELECT status FROM subscriptions WHERE start_date BETWEEN '2024-06-03' AND '2025-05-01'
