SELECT method, status, cnt
FROM (
    SELECT method, status, COUNT(*) cnt
    FROM payments
    WHERE amount <= 690
    GROUP BY method, status
) t
WHERE cnt < 3
ORDER BY cnt;
