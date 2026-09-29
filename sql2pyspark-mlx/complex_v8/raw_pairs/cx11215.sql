WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE source <> 'playlist'
),
latest AS (
    -- Keep the latest row per source, track_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        device_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, track_id ORDER BY ms_played DESC, id DESC) AS recency_rank
        FROM cleaned_plays
    ) ranked
    WHERE recency_rank = 1
),
combined_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device_slug,
        l.signup_date AS listener_signup_date,
        l.plan AS listener_plan,
        t.bpm AS track_bpm
    FROM latest s
    LEFT JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(ms_played) OVER (PARTITION BY track_id, listener_id) AS track_id_listener_id_sum_ms_played,
        ROUND(PERCENT_RANK() OVER w_track_id_by_listener_signup, 3) AS listener_signup_date_percent_rank,
        LAG(played_at, 2) OVER (PARTITION BY track_id ORDER BY listener_signup_date ASC, id) AS prev2_played_at,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id, listener_id), 2) AS ms_played_vs_track_id_listener_id_avg,
        MIN(track_bpm) OVER (PARTITION BY track_id, listener_id ORDER BY listener_signup_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_track_bpm_so_far,
        ROUND(track_bpm - AVG(track_bpm) OVER (PARTITION BY track_id, listener_id), 2) AS track_bpm_vs_track_id_listener_id_avg
    FROM combined_details_listener
    WINDOW w_track_id_by_listener_signup AS (PARTITION BY track_id ORDER BY listener_signup_date ASC)
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) FILTER (WHERE source IS NOT NULL) AS play_source_count
    FROM plays
    GROUP BY listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count
    FROM with_trends s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        play_source_count,
        YEAR(played_at) AS played_at_year,
        MAX(track_bpm_vs_track_id_listener_id_avg) AS max_track_bpm_vs_track_id_listener_id_avg,
        PERCENTILE_APPROX(track_id_listener_id_sum_ms_played, 0.5) AS median_track_id_listener_id_sum_ms_played,
        PERCENTILE_APPROX(track_bpm_vs_track_id_listener_id_avg, 0.5) AS median_track_bpm_vs_track_id_listener_id_avg,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(track_bpm_vs_track_id_listener_id_avg) / NULLIF(SUM(track_bpm), 0), 3) AS track_bpm_vs_track_id_listener_id_avg_to_track_b,
        COUNT(*) FILTER (WHERE listener_plan IN ('premium', 'student')) AS n_listener_plan_in_premium_stu
    FROM profiled
    GROUP BY play_source_count, YEAR(played_at)
)
-- Final projection
SELECT
    play_source_count,
    played_at_year,
    median_track_id_listener_id_sum_ms_played,
    distinct_ids,
    COALESCE(distinct_ids, 0) AS distinct_ids_filled,
    ABS(play_source_count - 2) AS play_source_count_distance
FROM summary
