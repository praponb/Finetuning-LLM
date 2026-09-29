WITH unioned AS (
    -- Stack the branches
    SELECT username AS name, signup_date AS event_date
    FROM listeners
    UNION ALL
    SELECT plan AS name, signup_date AS event_date
    FROM listeners
)
SELECT name, COUNT(*) AS n_rows, MAX(event_date) AS latest_event_date
FROM unioned
GROUP BY name
ORDER BY name
