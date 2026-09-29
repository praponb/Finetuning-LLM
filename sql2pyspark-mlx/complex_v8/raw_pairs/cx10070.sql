SELECT
    'policy' AS src,
    id,
    holder_id,
    end_date AS event_date
FROM policies
UNION
SELECT
    'policy_2' AS src,
    id,
    holder_id,
    start_date AS event_date
FROM policies
WHERE premium <= 3719.7
ORDER BY src DESC
