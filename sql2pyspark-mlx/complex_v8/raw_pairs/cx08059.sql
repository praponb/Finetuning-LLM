WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM plays
    WHERE ms_played >= 160264
),
enriched AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        l.birth_year AS listener_birth_year
    FROM cleaned s
    JOIN listeners l ON l.id = s.listener_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_listener_id_is_skipped_by_li AS listener_birth_year_rank,
        ROUND(listener_birth_year / NULLIF(SUM(listener_birth_year) OVER (PARTITION BY track_id), 0), 4) AS listener_birth_year_share_of_track_id,
        LAG(listener_birth_year, 2) OVER (PARTITION BY listener_id, is_skipped ORDER BY listener_birth_year NULLS LAST, id) AS prev2_listener_birth_year,
        LAG(listener_birth_year, 2, 0) OVER w_device_listener_id_by_listen AS prev2_listener_birth_year_2,
        ROUND(listener_birth_year - AVG(listener_birth_year) OVER (PARTITION BY device, is_skipped), 2) AS listener_birth_year_vs_device_is_skipped_avg,
        MIN(listener_birth_year) OVER (PARTITION BY device, listener_id) AS device_listener_id_min_listener_birth_year,
        listener_birth_year - LAG(listener_birth_year) OVER (PARTITION BY is_skipped ORDER BY listener_birth_year, id) AS listener_birth_year_change,
        COUNT(*) OVER (PARTITION BY device, is_skipped) AS rows_in_device_is_skipped
    FROM enriched
    WINDOW w_listener_id_is_skipped_by_li AS (PARTITION BY listener_id, is_skipped ORDER BY listener_birth_year NULLS LAST, id), w_device_listener_id_by_listen AS (PARTITION BY device, listener_id ORDER BY listener_birth_year, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(listener_birth_year / NULLIF(listener_birth_year_rank, 0), 3) AS listener_birth_year_per_listener_birth_year_rank
    FROM windowed
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(listener_birth_year_rank) OVER w_listener_id, 2) AS listener_id_avg_listener_birth_year_rank,
        LAG(listener_birth_year_change) OVER w_track_id_by_rows_in_device_i AS prev_listener_birth_year_change,
        DENSE_RANK() OVER w_device_by_listener_birth_yea AS listener_birth_year_share_of_track_id_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device ORDER BY listener_birth_year_share_of_track_id DESC), 3) AS listener_birth_year_share_of_track_id_percent_ra,
        MAX(listener_birth_year) OVER (PARTITION BY listener_id) AS listener_id_max_listener_birth_year
    FROM tagged
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_track_id_by_rows_in_device_i AS (PARTITION BY track_id ORDER BY rows_in_device_is_skipped, id), w_device_by_listener_birth_yea AS (PARTITION BY device ORDER BY listener_birth_year_share_of_track_id DESC)
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    is_skipped,
    listener_birth_year,
    listener_birth_year_share_of_track_id,
    prev2_listener_birth_year,
    prev2_listener_birth_year_2,
    listener_birth_year_vs_device_is_skipped_avg,
    device_listener_id_min_listener_birth_year,
    listener_birth_year_change,
    rows_in_device_is_skipped,
    listener_id_avg_listener_birth_year_rank,
    listener_birth_year_share_of_track_id_dense_rank,
    listener_birth_year_share_of_track_id_percent_ra,
    LENGTH(TRIM(device)) AS device_len,
    listener_birth_year + listener_birth_year_rank AS listener_birth_year_plus_listener_birth_year_ran
FROM with_windows
ORDER BY track_id DESC, id
LIMIT 20
