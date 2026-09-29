SELECT id, merchant, amount FROM (SELECT *, RANK() OVER (PARTITION BY merchant ORDER BY amount) rn FROM transactions WHERE merchant NOT IN ('Amazon', 'Shell')) r WHERE rn <= 5 ORDER BY id ASC;
