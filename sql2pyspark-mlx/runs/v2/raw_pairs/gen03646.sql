SELECT method, order_id, amount, TO_DATE(paid_at) AS paid_at_date FROM payments WHERE TO_DATE(paid_at) >= '2023-07-01' AND method IN ('card', 'paypal') AND amount > 1190
