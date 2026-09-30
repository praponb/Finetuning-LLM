SELECT status FROM orders WHERE discount > 0.35 UNION ALL SELECT status FROM payments WHERE amount >= 700;
