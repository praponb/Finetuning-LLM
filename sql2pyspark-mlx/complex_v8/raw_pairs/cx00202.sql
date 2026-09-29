WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source IN ('playlist', 'album')
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year,
        l.username AS listener_username
    FROM cleaned_plays s
    JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        played_at,
        device,
        listener_interests,
        listener_birth_year,
        listener_username,
        LAG(listener_birth_year) OVER (PARTITION BY is_skipped ORDER BY ms_played, id) AS prev_listener_birth_year,
        DENSE_RANK() OVER w_is_skipped_by_ms_played AS ms_played_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, track_id ORDER BY listener_birth_year, id), 3) AS listener_birth_year_cume_dist,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY is_skipped, source ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        SUM(listener_birth_year) OVER (PARTITION BY listener_id, track_id ORDER BY played_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS listener_birth_year_sum_7,
        LAG(listener_birth_year, 1, 0) OVER (PARTITION BY is_skipped, source ORDER BY played_at DESC, id) AS prev_listener_birth_year_2,
        NTILE(4) OVER (PARTITION BY listener_id, track_id ORDER BY played_at ASC, id) AS played_at_quartile,
        ROUND(AVG(listener_birth_year) OVER (PARTITION BY device, DATE_TRUNC('MONTH', played_at)), 2) AS device_played_at_month_avg_listener_birth_year
    FROM combined_details
    WINDOW w_is_skipped_by_ms_played AS (PARTITION BY is_skipped ORDER BY ms_played, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(listener_birth_year_sum_7) IGNORE NULLS OVER (PARTITION BY ms_played_dense_rank, DATE_TRUNC('MONTH', played_at) ORDER BY prev_listener_birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_listener_birth_year_sum_7,
        LAG(played_at, 2) OVER (PARTITION BY ms_played_dense_rank, DATE_TRUNC('MONTH', played_at) ORDER BY prev_listener_birth_year, id) AS prev2_played_at,
        ROUND(AVG(played_at_quartile) OVER (PARTITION BY played_at_quartile, ms_played_dense_rank), 2) AS played_at_quartile_ms_played_dense_rank_avg_play,
        ROUND(prev_listener_birth_year_2 - AVG(prev_listener_birth_year_2) OVER (PARTITION BY played_at_quartile, ms_played_dense_rank), 2) AS prev_listener_birth_year_2_vs_played_at_quartile,
        MAX(last_known_ms_played) OVER w_played_at_quartile_ms_played AS played_at_quartile_ms_played_dense_rank_max_last
    FROM ranked
    WINDOW w_played_at_quartile_ms_played AS (PARTITION BY played_at_quartile, ms_played_dense_rank)
)
SELECT
    id,
    track_id,
    played_at,
    device,
    listener_birth_year,
    listener_username,
    prev_listener_birth_year,
    ms_played_dense_rank,
    listener_birth_year_cume_dist,
    last_known_ms_played,
    listener_birth_year_sum_7,
    prev_listener_birth_year_2,
    played_at_quartile,
    last_known_listener_birth_year_sum_7,
    prev2_played_at,
    played_at_quartile_ms_played_dense_rank_avg_play,
    LEAST(ms_played_dense_rank, last_known_ms_played) AS least_ms_played_dense_rank_last_known_ms_played,
    COALESCE(device, 'UNKNOWN') AS device_or_unknown
FROM metrics
ORDER BY listener_birth_year_sum_7, id
