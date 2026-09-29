SELECT location FROM departments WHERE dept_name <> 'Finance' AND dept_name NOT IN ('HR', 'Legal', 'Engineering') AND location IN ('Bangkok', 'Chiang Mai') ORDER BY location DESC;
