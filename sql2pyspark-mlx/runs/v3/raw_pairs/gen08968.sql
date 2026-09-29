SELECT device FROM sessions WHERE started_at < '2024-03-01' UNION ALL SELECT device FROM sessions WHERE device IN ('tablet', 'desktop')
