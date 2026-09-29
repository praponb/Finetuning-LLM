SELECT track_id AS entity_id
FROM plays
EXCEPT
SELECT id
FROM tracks
WHERE is_explicit = FALSE
  AND release_date BETWEEN DATE '2024-05-12' AND DATE '2025-08-02'
ORDER BY entity_id
