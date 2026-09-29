SELECT id AS qty, device AS name
FROM plays
WHERE device = 'phone'
  AND is_skipped = FALSE
INTERSECT
SELECT birth_year AS qty, username AS name
FROM listeners
WHERE signup_date >= DATE '2024-10-07'
