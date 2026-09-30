SELECT policy_id
FROM claims
EXCEPT
SELECT level
FROM adjusters
WHERE level >= 3
  AND adjuster_name IS NOT NULL
ORDER BY policy_id DESC
