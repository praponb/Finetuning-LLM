SELECT id, location FROM departments WHERE location = 'Bangkok' UNION ALL SELECT id, location FROM departments WHERE location IN ('Bangkok', 'Singapore', 'Tokyo');
