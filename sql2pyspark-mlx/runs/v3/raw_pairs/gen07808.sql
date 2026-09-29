SELECT id, budget FROM departments WHERE location NOT IN ('Berlin', 'Chiang Mai', 'Tokyo') AND location IN ('London', 'Singapore') AND budget >= 3716000;
