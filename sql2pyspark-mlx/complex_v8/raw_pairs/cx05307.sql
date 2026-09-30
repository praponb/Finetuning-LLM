WITH combined AS (
    -- Stack the branches
    SELECT 'artist' AS origin, debut_year, id
    FROM artists
    WHERE genre <> 'HipHop'
    UNION
    SELECT 'listener' AS origin, birth_year AS debut_year, id
    FROM listeners
    WHERE country = 'UK'
)
SELECT origin, COUNT(*) AS n_rows, SUM(debut_year) AS total_debut_year
FROM combined
GROUP BY origin
ORDER BY origin
