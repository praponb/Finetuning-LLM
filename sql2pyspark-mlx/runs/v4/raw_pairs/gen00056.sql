SELECT status, MIN(amount) min_amount
FROM payments
WHERE amount >= 1330 OR amount <= 1420
GROUP BY status
HAVING MIN(amount) >= 480;
