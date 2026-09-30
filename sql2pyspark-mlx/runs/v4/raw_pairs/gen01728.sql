SELECT id, location, CASE WHEN dept_name = 'Finance' THEN 1 ELSE 0 END AS is_finance
FROM departments
WHERE budget < 1591000 AND budget BETWEEN 1604000 AND 1867000 AND location = 'Chiang Mai'
ORDER BY is_finance DESC, id ASC
LIMIT 10;
