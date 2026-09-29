SELECT budget, CASE WHEN location = 'Tokyo' THEN 1 ELSE 0 END AS is_tokyo
FROM departments
WHERE NOT (budget >= 1181000 AND dept_name IN ('Finance', 'Legal') AND budget BETWEEN 2428000 AND 3834000);
