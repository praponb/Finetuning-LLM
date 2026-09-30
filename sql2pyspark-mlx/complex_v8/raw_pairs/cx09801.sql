WITH stacked AS (
    -- Stack the branches
    SELECT ms_played AS ref_id, played_at AS happened_at
    FROM plays
    UNION ALL
    SELECT id, played_at
    FROM plays
    WHERE is_skipped = TRUE
)
SELECT ref_id, COUNT(*) AS n_rows, MAX(happened_at) AS latest_happened_at
FROM stacked
GROUP BY ref_id
ORDER BY ref_id
