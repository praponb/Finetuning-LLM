SELECT 'play' AS origin, ms_played AS key_id, played_at AS event_time
FROM plays
UNION ALL
SELECT 'play_2' AS origin, id, played_at
FROM plays
WHERE device = 'speaker'
