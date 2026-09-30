SELECT location FROM departments WHERE dept_name IN ('Engineering', 'Legal', 'Marketing') AND budget >= 3793000 AND dept_name IS NOT NULL ORDER BY location ASC
