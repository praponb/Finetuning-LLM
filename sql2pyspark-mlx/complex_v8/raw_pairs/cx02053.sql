SELECT birth_year AS key_id, signup_date AS event_date, 'listener' AS origin
FROM listeners
UNION ALL
SELECT bpm AS key_id, release_date AS event_date, 'track' AS origin
FROM tracks
