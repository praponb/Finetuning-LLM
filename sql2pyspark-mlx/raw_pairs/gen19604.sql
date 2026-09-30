SELECT id, started_at, browser FROM sessions WHERE NOT (started_at >= '2025-09-09' AND country <> 'SG' AND started_at >= '2023-07-01');
