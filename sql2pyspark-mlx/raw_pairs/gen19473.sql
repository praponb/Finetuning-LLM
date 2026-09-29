SELECT MAX(budget) - MIN(budget) AS budget_range, SUM(CASE WHEN location = 'Singapore' THEN 1 ELSE 0 END) AS singapore_count
FROM departments;
