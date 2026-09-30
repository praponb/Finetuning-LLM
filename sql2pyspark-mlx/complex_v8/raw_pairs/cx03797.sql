WITH stacked AS (
    -- Stack the branches
    SELECT artist_name, genre
    FROM artists
    WHERE country = 'JP'
    UNION ALL
    SELECT country AS artist_name, plan AS genre
    FROM listeners
    UNION ALL
    SELECT source AS artist_name, device AS genre
    FROM plays
    WHERE NOT is_skipped
      AND source NOT IN ('playlist', 'radio')
)
SELECT artist_name, COUNT(*) AS n_rows
FROM stacked
GROUP BY artist_name
ORDER BY artist_name
