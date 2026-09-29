SELECT device FROM sessions WHERE country = 'DE' INTERSECT SELECT device FROM sessions WHERE TO_DATE(started_at) < '2023-03-01'
