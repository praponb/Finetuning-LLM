SELECT policy_id
FROM claims
EXCEPT ALL
SELECT level AS policy_id
FROM adjusters
WHERE team <> 'Travel'
  AND adjuster_name IS NOT NULL
ORDER BY policy_id
