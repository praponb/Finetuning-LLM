WITH unioned AS (
    -- Stack the branches
    SELECT plan AS label, username AS category, signup_date AS day
    FROM listeners
    UNION ALL
    SELECT plan, country, signup_date
    FROM listeners
)
SELECT label, COUNT(*) AS n_rows, MAX(day) AS latest_day
FROM unioned
GROUP BY label
ORDER BY label
