WITH base AS (
    SELECT *
    FROM customers
    WHERE country = 'SG' AND city != 'Chiang Mai'
),
segment_summary AS (
    SELECT segment, AVG(age) AS avg_age
    FROM base
    GROUP BY segment
)
SELECT segment, avg_age
FROM segment_summary
WHERE avg_age < 36
ORDER BY avg_age DESC;
