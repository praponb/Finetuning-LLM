WITH stacked AS (
    -- Stack the branches
    SELECT birth_year AS ref_id, plan
    FROM listeners
    UNION ALL
    SELECT id, country
    FROM listeners
)
SELECT plan, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM stacked
GROUP BY plan
ORDER BY plan
