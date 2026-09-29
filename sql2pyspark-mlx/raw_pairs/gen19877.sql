SELECT id, location, FLOOR(budget) AS budget_whole FROM departments WHERE location IN ('Singapore', 'Chiang Mai') AND budget <= 3315000 ORDER BY id DESC, location DESC
