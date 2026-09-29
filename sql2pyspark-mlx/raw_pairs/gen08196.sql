SELECT merchant, COUNT(DISTINCT amount) AS unique_merchants FROM transactions WHERE is_flagged GROUP BY merchant ORDER BY merchant, 2 DESC;
