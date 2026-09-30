SELECT location
FROM departments
WHERE budget BETWEEN 2675000 AND 3063000 OR location IN ('Tokyo', 'London', 'Bangkok');
