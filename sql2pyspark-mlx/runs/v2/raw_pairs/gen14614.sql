SELECT paid_at, amount, order_id FROM payments WHERE method IS NULL AND amount BETWEEN 700 AND 1380 ORDER BY amount DESC
