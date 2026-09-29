WITH unioned AS (
    -- Stack the branches
    SELECT title AS label, bpm AS qty
    FROM tracks
    UNION ALL
    SELECT artist_name AS label, id AS qty
    FROM artists
    WHERE is_verified = TRUE
)
SELECT label, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM unioned
GROUP BY label
ORDER BY label
