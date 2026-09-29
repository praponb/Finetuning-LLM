SELECT method, SUM(amount) AS total_amount
FROM payments
WHERE method = 'bank_transfer' OR method IN ('card', 'cash', 'promptpay')
GROUP BY method
HAVING SUM(amount) < 9200
ORDER BY 1 ASC, total_amount DESC
