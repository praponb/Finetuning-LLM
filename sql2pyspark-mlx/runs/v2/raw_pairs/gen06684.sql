SELECT id, started_at, YEAR(started_at) AS started_at_year FROM sessions WHERE TO_DATE(started_at) < '2024-12-01'
