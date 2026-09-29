SELECT artist_id, duration_sec AS ref_id
FROM tracks
WHERE LENGTH(TRIM(title)) >= 6
  AND duration_sec < 357
UNION ALL
SELECT artist_id, bpm
FROM tracks
