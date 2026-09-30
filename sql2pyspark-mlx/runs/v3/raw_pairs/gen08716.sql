select id, amount, method from payments where method not in ('cash', 'promptpay', 'bank_transfer') and (order_id in (23, 24, 34, 38) or paid_at >= '2024-01-01');
