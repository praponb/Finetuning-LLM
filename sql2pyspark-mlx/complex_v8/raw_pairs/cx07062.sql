WITH all_events AS (
    -- Stack the branches
    SELECT country AS name, debut_year AS qty, 'artist' AS record_type
    FROM artists
    WHERE UPPER(TRIM(artist_name)) LIKE 'N%'
    UNION ALL
    SELECT plan, id, 'listener' AS record_type
    FROM listeners
    WHERE country IN ('BR', 'TH', 'KR')
      AND plan = 'premium'
    UNION ALL
    SELECT username, id, 'listener' AS record_type
    FROM listeners
    WHERE signup_date >= DATE '2024-10-07'
)
SELECT record_type, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM all_events
GROUP BY record_type
ORDER BY record_type
