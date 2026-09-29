SELECT *
FROM (
    SELECT method, SUM(CASE WHEN method = 'card' THEN amount ELSE 0 END) AS card_amount, SUM(amount) AS total_amount
    FROM payments
    GROUP BY method
) AS x
WHERE card_amount < 43;
