WITH scoped AS (
    -- Filter tracks
    SELECT *
    FROM tracks
    WHERE bpm >= 75
      AND LOWER(title) LIKE '%ne%'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY is_explicit, artist_id), 0), 4) AS duration_sec_share_of_is_explicit_artist_id,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id) AS rows_in_is_explicit_artist_id
    FROM scoped
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, bpm, rows_in_is_explicit_artist_id, duration_sec
    FROM with_windows
) src
UNPIVOT (
    metric_value FOR metric_name IN (bpm, rows_in_is_explicit_artist_id, duration_sec)
) unp
