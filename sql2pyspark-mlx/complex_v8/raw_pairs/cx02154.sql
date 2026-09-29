SELECT duration_sec AS key_id, id, release_date AS event_date
FROM tracks
UNION
SELECT duration_sec AS key_id, bpm AS id, release_date AS event_date
FROM tracks
