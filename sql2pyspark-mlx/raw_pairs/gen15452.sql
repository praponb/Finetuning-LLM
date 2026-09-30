SELECT id, paid_at, method, SUBSTRING(method, 1, 4) AS method_prefix FROM payments WHERE amount BETWEEN 800 AND 1390 AND method IN ('card', 'promptpay') ORDER BY method DESC
