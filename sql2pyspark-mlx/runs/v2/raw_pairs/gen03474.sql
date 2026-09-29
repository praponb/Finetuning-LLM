SELECT device FROM sessions WHERE started_at < '2024-05-01' UNION ALL SELECT device FROM sessions WHERE country = 'UK' ORDER BY device;
