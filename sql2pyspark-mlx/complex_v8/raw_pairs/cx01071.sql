WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE artist_id <= 12
      AND bpm >= 96
)
SELECT artist_id, title AS label
FROM tracks
EXCEPT ALL
SELECT artist_id, title
FROM tracks
