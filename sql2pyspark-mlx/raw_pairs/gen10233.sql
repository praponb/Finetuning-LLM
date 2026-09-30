SELECT merchant, SUM(CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END) AS 7_eleven_count, ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
GROUP BY 1
ORDER BY 7_eleven_count DESC
