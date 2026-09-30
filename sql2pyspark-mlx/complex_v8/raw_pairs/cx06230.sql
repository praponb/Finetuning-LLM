WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device
    FROM plays
    WHERE device <> 'desktop'
      AND played_at < TIMESTAMP '2025-02-18 12:00:00'
),
with_parent AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
    WHERE ARRAY_CONTAINS(l.interests, 'jazz')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        listener_interests,
        listener_birth_year,
        FIRST_VALUE(listener_birth_year) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY listener_birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_listener_birth_year,
        SUM(listener_birth_year) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY listener_birth_year, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS listener_birth_year_sum_2,
        ROUND(listener_birth_year / NULLIF(SUM(listener_birth_year) OVER w_listener_id_track_id, 0), 4) AS listener_birth_year_share_of_listener_id_track_i,
        COUNT(*) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER (ORDER BY listener_birth_year, id), 3) AS listener_birth_year_cume_dist,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id)) AS distinct_track_id_in_listener_id,
        LAST_VALUE(listener_birth_year) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY listener_birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_listener_birth_year,
        ROUND(AVG(listener_birth_year) OVER (ORDER BY played_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS listener_birth_year_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY listener_birth_year, id) AS seq_no
    FROM with_parent
    WINDOW w_listener_id_track_id AS (PARTITION BY listener_id, track_id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        listener_interests,
        listener_birth_year,
        first_known_listener_birth_year,
        listener_birth_year_sum_2,
        listener_birth_year_share_of_listener_id_track_i,
        running_count,
        listener_birth_year_cume_dist,
        distinct_track_id_in_listener_id,
        last_known_listener_birth_year,
        listener_birth_year_avg_2_prior
    FROM with_windows
    WHERE seq_no <= 5
      AND played_at < TIMESTAMP '2025-03-02 10:00:00'
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    listener_interests,
    listener_birth_year,
    first_known_listener_birth_year,
    listener_birth_year_sum_2,
    listener_birth_year_share_of_listener_id_track_i,
    running_count,
    distinct_track_id_in_listener_id,
    listener_birth_year_avg_2_prior,
    GREATEST(listener_birth_year, running_count) AS greatest_listener_birth_year_running_count,
    COALESCE(listener_birth_year, 0) AS listener_birth_year_filled
FROM top_per_group
WHERE listener_birth_year_sum_2 BETWEEN 2366 AND 5414
ORDER BY track_id
