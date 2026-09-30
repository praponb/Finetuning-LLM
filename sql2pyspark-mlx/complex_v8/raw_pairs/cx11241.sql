SELECT 'listener' AS record_type, id, birth_year AS key_id
FROM listeners
UNION
SELECT 'play' AS record_type, id, ms_played
FROM plays
WHERE listener_id >= 18
ORDER BY key_id, id
