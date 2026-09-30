SELECT budget, location
FROM departments
WHERE budget >= 2851000 AND location = 'Berlin' AND location IS NOT NULL;
