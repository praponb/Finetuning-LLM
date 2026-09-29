select * from sessions where (started_at < '2024-11-01' and browser = 'Chrome') or started_at < '2025-07-09' order by device asc;
