SELECT status, MAX(amount) AS max_amount, SUM(amount) AS total_amount FROM payments WHERE amount < 790 OR method IN ('paypal', 'card', 'cash') GROUP BY status;
