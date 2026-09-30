SELECT id, budget, ROW_NUMBER() OVER (ORDER BY budget, id) AS budget_row FROM departments WHERE location IN ('Singapore', 'Tokyo') ORDER BY id DESC, budget DESC
