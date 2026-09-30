SELECT status AS category
FROM policies
WHERE premium <= 3719.7
  AND end_date < DATE '2026-07-11'
INTERSECT
SELECT adjuster_name
FROM adjusters
ORDER BY category DESC
