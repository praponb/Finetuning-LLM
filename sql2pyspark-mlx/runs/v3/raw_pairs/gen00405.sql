SELECT SUM(CASE WHEN location = 'Chiang Mai' THEN budget ELSE 0 END) chiang_mai_budget, COUNT(*) row_count FROM departments ORDER BY 1 DESC;
