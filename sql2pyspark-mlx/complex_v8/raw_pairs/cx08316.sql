WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE duration_sec >= 142
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id DESC) AS row_num
        FROM filtered_tracks
    ) ranked
    WHERE row_num = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far,
        LEAD(bpm, 1, 0) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id) AS next_bpm,
        SUM(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        bpm - LAG(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id) AS bpm_change,
        DENSE_RANK() OVER w_is_explicit_by_bpm_ties AS bpm_dense_rank
    FROM deduped
    WINDOW w_is_explicit_by_bpm_ties AS (PARTITION BY is_explicit ORDER BY bpm DESC)
)
-- Final projection
SELECT
    id,
    artist_id,
    is_explicit,
    bpm,
    moods,
    max_bpm_so_far,
    next_bpm,
    running_bpm,
    bpm_change,
    bpm_dense_rank,
    ROUND(bpm_change / NULLIF(next_bpm, 0), 3) AS bpm_change_per_next_bpm
FROM windowed
ORDER BY id NULLS LAST
