SELECT id, budget FROM departments WHERE NOT (budget IS NOT NULL AND location NOT IN ('London', 'Berlin', 'New York'));
