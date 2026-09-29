WITH stacked AS (
    -- Stack the branches
    SELECT 'play' AS source, device AS category, played_at AS happened_at
    FROM plays
    UNION ALL
    SELECT 'play_2' AS source, device AS category, played_at AS happened_at
    FROM plays
)
SELECT source, COUNT(*) AS n_rows, MAX(happened_at) AS latest_happened_at
FROM stacked
GROUP BY source
ORDER BY source
