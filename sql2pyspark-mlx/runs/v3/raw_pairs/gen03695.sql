SELECT DISTINCT method FROM payments WHERE method IN ('promptpay', 'card', 'bank_transfer') ORDER BY method;
