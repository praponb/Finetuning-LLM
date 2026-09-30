SELECT segment, country, COUNT(*) AS cnt FROM customers GROUP BY segment, country HAVING COUNT(*) < 2 ORDER BY cnt;
