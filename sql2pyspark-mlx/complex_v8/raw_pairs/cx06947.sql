WITH base AS (
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
    WHERE device <> 'speaker'
),
joined_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        t.is_explicit AS track_is_explicit,
        l.signup_date AS listener_signup_date
    FROM base s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at,
        is_skipped,
        track_is_explicit,
        listener_signup_date,
        MIN(ms_played) OVER (PARTITION BY listener_id, track_is_explicit) AS listener_id_track_is_explicit_min_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_listener_id_track_is_explici, 0), 4) AS ms_played_share_of_listener_id_track_is_explicit,
        DENSE_RANK() OVER w_track_is_explicit_by_played AS played_at_dense_rank,
        RANK() OVER w_device_by_played_at AS played_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id ORDER BY ms_played DESC NULLS FIRST, id), 3) AS ms_played_cume_dist,
        MAX(ms_played) OVER (PARTITION BY track_is_explicit ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ms_played_max_3_prior,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY listener_id), 0), 4) AS ms_played_share_of_listener_id,
        DENSE_RANK() OVER (PARTITION BY track_is_explicit ORDER BY played_at DESC NULLS FIRST) AS played_at_dense_rank_2
    FROM joined_track
    WINDOW w_listener_id_track_is_explici AS (PARTITION BY listener_id, track_is_explicit), w_track_is_explicit_by_played AS (PARTITION BY track_is_explicit ORDER BY played_at DESC NULLS FIRST, id), w_device_by_played_at AS (PARTITION BY device ORDER BY played_at DESC, id)
)
SELECT
    id,
    listener_id,
    played_at,
    is_skipped,
    track_is_explicit,
    listener_signup_date,
    listener_id_track_is_explicit_min_ms_played,
    ms_played_share_of_listener_id_track_is_explicit,
    played_at_rank,
    ms_played_cume_dist,
    ms_played_max_3_prior,
    played_at_dense_rank_2,
    COALESCE(ms_played_cume_dist, 0.0) AS ms_played_cume_dist_filled
FROM sequenced
WHERE ms_played_max_3_prior >= 244000
ORDER BY ms_played_max_3_prior DESC
