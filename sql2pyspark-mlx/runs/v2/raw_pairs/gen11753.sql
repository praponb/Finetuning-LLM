SELECT merchant, FLOOR(amount) AS amount_whole, LOWER(merchant) AS merchant_lower
FROM transactions
WHERE merchant IN ('Starbucks', 'Lazada', 'Grab') AND merchant = 'Grab'
ORDER BY merchant_lower DESC;
