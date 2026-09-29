WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source
    FROM plays
    WHERE ms_played BETWEEN 26344 AND 246779
      AND is_skipped = TRUE
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY source, is_skipped ORDER BY played_at DESC NULLS FIRST), 3) AS played_at_percent_rank,
        LAG(played_at) OVER w_source_is_skipped_by_played_2 AS prev_played_at,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY source ORDER BY played_at DESC, id))) / 60, 1) AS minutes_since_prev_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY source ORDER BY played_at DESC), 3) AS played_at_percent_rank_2,
        NTILE(10) OVER (PARTITION BY source ORDER BY played_at DESC, id) AS played_at_decile
    FROM scoped_plays
    WINDOW w_source_is_skipped_by_played_2 AS (PARTITION BY source, is_skipped ORDER BY played_at DESC NULLS FIRST, id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        played_at_percent_rank,
        prev_played_at,
        played_at_percent_rank_2,
        played_at_decile,
        LAG(played_at_decile) OVER (PARTITION BY listener_id, TO_DATE(prev_played_at) ORDER BY played_at_percent_rank_2 DESC, id) AS prev_played_at_decile,
        MAX(played_at_percent_rank_2) OVER (PARTITION BY YEAR(played_at), track_id ORDER BY played_at_percent_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS played_at_percent_rank_2_max_3
    FROM with_trends
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(prev_played_at, 'yyyy-MM-dd HH:00') AS prev_played_at_label
    FROM windowed
    WHERE track_id IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT listener_id, prev_played_at_decile, source, played_at_percent_rank_2_max_3
    FROM derived
) src
PIVOT (
    SUM(played_at_percent_rank_2_max_3)
    FOR source IN ('playlist', 'search', 'album', 'radio')
)
ORDER BY listener_id, prev_played_at_decile
