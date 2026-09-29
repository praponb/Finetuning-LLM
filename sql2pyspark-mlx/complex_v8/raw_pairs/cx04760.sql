WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date
    FROM tracks
    WHERE is_explicit = FALSE
      AND ARRAY_CONTAINS(moods, 'party')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        duration_sec,
        is_explicit,
        bpm,
        duration_sec_filled,
        days_since_release_date,
        RANK() OVER w_is_explicit_artist_id_by_dur AS duration_sec_rank,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC), 3) AS duration_sec_percent_rank,
        ROUND(duration_sec_filled / NULLIF(SUM(duration_sec_filled) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS duration_sec_filled_share_of_artist_id_is_explic,
        ROUND(AVG(days_since_release_date) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS days_since_release_date_avg_1_prior
    FROM scoped_tracks
    WINDOW w_is_explicit_artist_id_by_dur AS (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC)
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at
    FROM with_windows s
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM profiled
    WHERE duration_sec_rank = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        duration_sec,
        is_explicit,
        bpm,
        days_since_release_date,
        duration_sec_rank,
        first_known_bpm,
        duration_sec_percent_rank,
        LAST_VALUE(first_known_bpm) OVER (PARTITION BY duration_sec_rank, DATE_TRUNC('MONTH', first_played_at) ORDER BY duration_sec_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_known_bpm,
        SUM(first_known_bpm) OVER (ORDER BY last_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_first_known_bpm,
        MAX(days_since_release_date) OVER (PARTITION BY duration_sec_rank, DATE_TRUNC('MONTH', first_played_at) ORDER BY duration_sec_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_days_since_release_date_so_far,
        LAG(last_played_at) OVER w_all_by_last_played_at AS prev_last_played_at,
        LAST_VALUE(first_known_bpm) OVER (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_known_bpm_2,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY last_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec
    FROM top_rows
    WINDOW w_all_by_last_played_at AS (ORDER BY last_played_at, id)
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count
    FROM windowed s
)
-- Final projection
SELECT
    id,
    duration_sec,
    is_explicit,
    bpm,
    days_since_release_date,
    duration_sec_rank,
    first_known_bpm,
    duration_sec_percent_rank,
    last_first_known_bpm,
    running_first_known_bpm,
    second_duration_sec,
    last_played_at,
    play_count,
    ABS(last_first_known_bpm_2 - 108) AS last_first_known_bpm_2_distance
FROM summarized_rows
