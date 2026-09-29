SELECT location FROM departments WHERE budget <= 2856000 UNION ALL SELECT location FROM departments WHERE budget IS NOT NULL ORDER BY location DESC;
