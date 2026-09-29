SELECT track_id AS ref_id, source
FROM plays
WHERE played_at < TIMESTAMP '2025-02-18 12:00:00'
  AND listener_id IS NOT NULL
INTERSECT
SELECT birth_year, plan
FROM listeners
WHERE SIZE(interests) >= 2
