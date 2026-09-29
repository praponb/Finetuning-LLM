SELECT
    budget_bucket,
    location,
    pct_london,
    unique_locations
FROM (
    SELECT
        CASE WHEN budget >= 3841000 THEN 'high' WHEN budget >= 1900000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        location,
        ROUND((100 * SUM(CASE WHEN location = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london,
        COUNT(DISTINCT budget) AS unique_locations
    FROM departments
    GROUP BY CASE WHEN budget >= 3841000 THEN 'high' WHEN budget >= 1900000 THEN 'mid' ELSE 'low' END, location
) AS t
WHERE pct_london < 27;
