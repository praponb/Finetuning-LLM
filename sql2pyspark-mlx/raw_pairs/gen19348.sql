SELECT *, CASE WHEN location IN ('New York', 'Bangkok') THEN 'Core' ELSE 'Other' END AS location_group, ROUND(budget, 2) AS budget_rounded
FROM departments
WHERE budget < 1947000;
