SELECT merchant, MIN(amount) AS min_amount FROM transactions GROUP BY merchant ORDER BY min_amount DESC;
