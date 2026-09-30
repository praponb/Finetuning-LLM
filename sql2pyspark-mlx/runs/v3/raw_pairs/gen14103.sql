SELECT id, location, COALESCE(budget, 0) budget_filled, ROUND(budget * 0.9, 2) budget_scaled FROM departments WHERE location IN ('Bangkok', 'Singapore', 'Berlin')
