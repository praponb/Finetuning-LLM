WITH unioned AS (
    -- Stack the branches
    SELECT id, source AS label
    FROM plays
    UNION
    SELECT id, plan
    FROM listeners
    WHERE ARRAY_CONTAINS(interests, 'jazz')
)
SELECT label, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM unioned
GROUP BY label
ORDER BY label
