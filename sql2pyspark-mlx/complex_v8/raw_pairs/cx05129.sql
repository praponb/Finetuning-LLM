WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
    FROM plays
    WHERE ms_played < 302279
      AND listener_id <= 12
),
stacked AS (
    -- Stack the branches
    SELECT
        ms_played AS qty,
        played_at,
        device,
        'play' AS src
    FROM recent
    UNION ALL
    SELECT
        id,
        played_at,
        device,
        'play_2' AS src
    FROM plays
    WHERE source IN ('playlist', 'radio')
      AND track_id IS NOT NULL
    UNION ALL
    SELECT
        ms_played,
        played_at,
        device,
        'play_3' AS src
    FROM plays
)
SELECT src, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(played_at) AS latest_played_at
FROM stacked
GROUP BY src
ORDER BY src
