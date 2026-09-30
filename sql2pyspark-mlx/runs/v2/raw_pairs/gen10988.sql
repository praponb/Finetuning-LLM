SELECT merchant, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM transactions
WHERE is_flagged AND (merchant = 'Starbucks' OR amount < 3600)
GROUP BY merchant
HAVING stddev_amount < 78
