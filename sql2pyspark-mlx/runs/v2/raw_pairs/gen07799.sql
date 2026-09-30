SELECT id, budget, location FROM departments WHERE budget = (SELECT MIN(budget) FROM departments WHERE location = 'Bangkok')
