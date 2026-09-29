SELECT holder_id, premium AS value, 'policy' AS record_type
FROM policies
UNION ALL
SELECT id, premium, 'policy_2' AS record_type
FROM policies
ORDER BY holder_id DESC, value
