SELECT *
FROM departments
WHERE location <> 'Chiang Mai' AND location <> 'London' AND location IN ('Bangkok', 'Tokyo')
ORDER BY location;
