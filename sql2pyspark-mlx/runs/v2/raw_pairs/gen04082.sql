SELECT method FROM payments WHERE amount <= 760 EXCEPT SELECT method FROM payments WHERE TO_DATE(paid_at) >= '2024-06-01';
