WITH unioned AS (
    -- Stack the branches
    SELECT id AS ref_id, level AS key_id, 'adjuster' AS src
    FROM adjusters
    UNION
    SELECT policy_id AS ref_id, id AS key_id, 'claim' AS src
    FROM claims
    WHERE claimed_amount >= 19347.2
)
SELECT src, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM unioned
GROUP BY src
ORDER BY src
