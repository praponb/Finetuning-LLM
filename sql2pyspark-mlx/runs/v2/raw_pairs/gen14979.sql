SELECT opened_date, region
FROM stores
WHERE opened_date BETWEEN '2023-12-01' AND '2025-09-01'
UNION
SELECT opened_date, region
FROM stores
WHERE YEAR(opened_date) >= 2024
ORDER BY opened_date DESC
