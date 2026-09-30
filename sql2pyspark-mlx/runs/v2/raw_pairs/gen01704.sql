SELECT segment, age_bucket, unique_segments
FROM (
    SELECT segment, CASE WHEN age >= 58 THEN 'high' WHEN age >= 42 THEN 'mid' ELSE 'low' END age_bucket, COUNT(DISTINCT age) unique_segments
    FROM customers
    WHERE UPPER(TRIM(email)) = 'LIAM@OUTLOOK.COM'
    GROUP BY segment, CASE WHEN age >= 58 THEN 'high' WHEN age >= 42 THEN 'mid' ELSE 'low' END
) sub
WHERE unique_segments > 7
ORDER BY unique_segments, segment, age_bucket
LIMIT 1;
