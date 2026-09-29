SELECT source AS name, played_at
FROM plays
UNION ALL
SELECT source, played_at
FROM plays
UNION ALL
SELECT device, played_at
FROM plays
