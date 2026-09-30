WITH filtered AS (
    -- Scope the tracks rows
    SELECT *
    FROM tracks
    WHERE is_explicit = FALSE
      AND UPPER(TRIM(title)) LIKE 'R%'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        MAX(bpm) OVER w_artist_id_is_explicit AS artist_id_is_explicit_max_bpm,
        DENSE_RANK() OVER (ORDER BY duration_sec DESC) AS duration_sec_dense_rank,
        ROUND(bpm - AVG(bpm) OVER w_artist_id_is_explicit, 2) AS bpm_vs_artist_id_is_explicit_avg
    FROM filtered
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit)
),
filtered_2 AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND HOUR(p.played_at) BETWEEN 9 AND 12
              AND p.is_skipped = TRUE
        )
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        artist_id_is_explicit_max_bpm,
        duration_sec_dense_rank,
        SUM(duration_sec) OVER (PARTITION BY is_explicit, duration_sec_dense_rank) AS is_explicit_duration_sec_dense_rank_sum_duration,
        DENSE_RANK() OVER w_duration_sec_dense_rank_rele AS bpm_dense_rank
    FROM filtered_2
    WINDOW w_duration_sec_dense_rank_rele AS (PARTITION BY duration_sec_dense_rank, MONTH(release_date) ORDER BY bpm)
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count
    FROM metrics_2 s
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.release_date,
        s.is_explicit,
        s.duration_sec_dense_rank,
        s.first_played_at,
        s.play_count,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped
    FROM summarized_rows s
    JOIN plays p ON p.track_id = s.id
),
without_matches AS (
    -- Keep rows whose play matches
    SELECT s.*
    FROM with_children s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.id = s.play_id
              AND p.ms_played BETWEEN 56858 AND 239955
        )
)
SELECT
    play_id,
    play_count,
    play_is_skipped,
    ROUND(STDDEV(duration_sec_dense_rank), 2) AS stddev_duration_sec_dense_rank,
    MAX(release_date) AS last_release_date
FROM without_matches
GROUP BY CUBE(play_id, play_count, play_is_skipped)
ORDER BY play_id NULLS LAST, play_count NULLS LAST, play_is_skipped NULLS LAST
