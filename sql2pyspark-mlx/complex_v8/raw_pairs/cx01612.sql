SELECT ms_played AS ref_id
FROM plays
WHERE played_at < TIMESTAMP '2025-02-24 09:00:00'
MINUS
SELECT track_id AS ref_id
FROM plays
