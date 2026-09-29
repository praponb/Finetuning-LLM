SELECT id, budget, INITCAP(TRIM(location)) AS location_title, COALESCE(budget, 0) AS budget_filled FROM departments WHERE location != 'Berlin';
