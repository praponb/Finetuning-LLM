WITH unioned AS (
    -- Stack the branches
    SELECT id AS qty, device
    FROM plays
    WHERE source IN ('search', 'radio')
      AND is_skipped = TRUE
    UNION ALL
    SELECT id, genre
    FROM artists
)
SELECT device, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM unioned
GROUP BY device
ORDER BY device
