WITH scoped AS (
    -- Filter plays
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
    WHERE device IN ('tv', 'car', 'phone')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY device ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY source ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY device), 2) AS ms_played_vs_device_avg,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id, MONTH(played_at)), 2) AS ms_played_vs_track_id_played_at_month_avg,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        MAX(ms_played) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_max_2,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC NULLS FIRST), 3) AS ms_played_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY source ORDER BY played_at DESC), 3) AS played_at_cume_dist,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY is_skipped, source), 0), 4) AS ms_played_share_of_is_skipped_source,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY played_at DESC, id) AS seq_no,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played_2
    FROM scoped
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        first_known_ms_played,
        last_known_ms_played,
        ms_played_vs_device_avg,
        ms_played_vs_track_id_played_at_month_avg,
        second_played_at,
        ms_played_max_2,
        ms_played_cume_dist,
        played_at_cume_dist,
        ms_played_share_of_is_skipped_source,
        last_known_ms_played_2
    FROM sequenced
    WHERE seq_no <= 2
      AND first_known_ms_played >= 52009
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        NTH_VALUE(ms_played_cume_dist, 2) OVER (PARTITION BY is_skipped ORDER BY played_at_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_cume_dist,
        LAST_VALUE(ms_played_vs_device_avg) OVER (ORDER BY second_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played_vs_device_avg,
        LEAD(ms_played_max_2) OVER (ORDER BY second_played_at, id) AS next_ms_played_max_2,
        COUNT(*) OVER w_is_skipped AS rows_in_is_skipped,
        ROUND(played_at_cume_dist / NULLIF(SUM(played_at_cume_dist) OVER (PARTITION BY is_skipped), 0), 4) AS played_at_cume_dist_share_of_is_skipped
    FROM leaders
    WINDOW w_is_skipped AS (PARTITION BY is_skipped)
)
SELECT
    id,
    track_id,
    played_at,
    ms_played,
    source,
    ms_played_vs_track_id_played_at_month_avg,
    ms_played_max_2,
    ms_played_cume_dist,
    played_at_cume_dist,
    ms_played_share_of_is_skipped_source,
    second_ms_played,
    second_ms_played_cume_dist,
    last_ms_played_vs_device_avg,
    next_ms_played_max_2,
    rows_in_is_skipped,
    played_at_cume_dist_share_of_is_skipped,
    CASE WHEN ms_played >= 221269 THEN 'large' WHEN ms_played >= 73335 THEN 'medium' ELSE 'small' END AS ms_played_tier,
    FLOOR(next_ms_played_max_2 / 1000000) * 1000000 AS next_ms_played_max_2_bucket
FROM ranked
ORDER BY ms_played DESC
