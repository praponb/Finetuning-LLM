SELECT status, total_amount, card_count
FROM (
    SELECT status, SUM(amount) AS total_amount, COUNT(CASE WHEN method = 'card' THEN 1 END) AS card_count
    FROM payments
    WHERE amount BETWEEN 900 AND 1210
    GROUP BY status
) AS agg
WHERE card_count > 3;
