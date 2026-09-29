SELECT location FROM departments WHERE budget IS NOT NULL UNION ALL SELECT location FROM departments WHERE location = 'Singapore';
