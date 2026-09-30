SELECT id, budget, location FROM departments WHERE budget >= (SELECT AVG(budget) FROM departments WHERE location IN ('Bangkok', 'New York', 'Chiang Mai')) ORDER BY id, location DESC;
