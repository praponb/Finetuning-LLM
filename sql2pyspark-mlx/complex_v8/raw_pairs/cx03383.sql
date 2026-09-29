WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE is_explicit = TRUE
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        is_explicit,
        bpm,
        moods,
        DENSE_RANK() OVER (PARTITION BY is_explicit ORDER BY release_date) AS release_date_dense_rank,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit
    FROM cleaned_tracks
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.is_skipped = FALSE
              AND p.ms_played > 166517
        )
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        is_explicit,
        bpm,
        release_date_dense_rank,
        rows_in_is_explicit,
        ROUND(release_date_dense_rank / NULLIF(SUM(release_date_dense_rank) OVER w_is_explicit_release_date_den, 0), 4) AS release_date_dense_rank_share_of_is_explicit_rel,
        LAG(rows_in_is_explicit, 1, 0) OVER w_release_date_dense_rank_is_e AS prev_rows_in_is_explicit,
        LEAD(bpm, 1, 0) OVER w_release_date_dense_rank_is_e AS next_bpm,
        SUM(rows_in_is_explicit) OVER (PARTITION BY release_date_dense_rank, is_explicit ORDER BY bpm ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rows_in_is_explicit
    FROM filtered
    WINDOW w_is_explicit_release_date_den AS (PARTITION BY is_explicit, release_date_dense_rank), w_release_date_dense_rank_is_e AS (PARTITION BY release_date_dense_rank, is_explicit ORDER BY bpm ASC NULLS LAST, id)
)
SELECT id, metric_name, amount
FROM (
    SELECT id, bpm, running_rows_in_is_explicit
    FROM windowed
) src
UNPIVOT (
    amount FOR metric_name IN (bpm AS bpm_v, running_rows_in_is_explicit AS running_rows_in_is_explicit_v)
) unp
ORDER BY id, metric_name
