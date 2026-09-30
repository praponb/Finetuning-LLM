WITH combined AS (
    -- Stack the branches
    SELECT artist_name, id AS key_id
    FROM artists
    UNION ALL
    SELECT device, id
    FROM plays
    WHERE listener_id <= 15
    UNION ALL
    SELECT username, id
    FROM listeners
    WHERE SIZE(interests) >= 2
)
SELECT artist_name, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id
FROM combined
GROUP BY artist_name
ORDER BY artist_name
