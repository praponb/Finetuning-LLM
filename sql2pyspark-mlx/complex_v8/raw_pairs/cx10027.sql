WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        DAYOFWEEK(release_date) AS release_date_dayofweek
    FROM tracks
    WHERE release_date >= DATE '2024-09-26'
)
SELECT id AS ref_id, title AS category
FROM tracks
EXCEPT ALL
SELECT track_id, source
FROM plays
WHERE played_at < TIMESTAMP '2025-02-24 09:00:00'
  AND ms_played < 223262
