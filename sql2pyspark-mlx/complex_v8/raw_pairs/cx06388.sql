WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LOWER(TRIM(device)) AS device_clean
    FROM plays
    WHERE track_id >= 28
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.device_clean,
        l.country AS listener_country,
        t.moods AS track_moods,
        t.is_explicit AS track_is_explicit
    FROM recent_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        device_clean,
        listener_country,
        track_is_explicit,
        FIRST_VALUE(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        MAX(ms_played) OVER (PARTITION BY track_id ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id ORDER BY ms_played DESC), 3) AS ms_played_cume_dist,
        RANK() OVER w_all_by_played_at_ties AS played_at_rank,
        NTILE(10) OVER (ORDER BY played_at, id) AS played_at_decile,
        NTILE(5) OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id) AS ms_played_quintile,
        DENSE_RANK() OVER w_all_by_played_at_ties AS played_at_dense_rank,
        COUNT(*) OVER (PARTITION BY track_is_explicit) AS rows_in_track_is_explicit,
        NTH_VALUE(ms_played, 2) OVER (ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY track_id ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        ROUND(AVG(ms_played) OVER (ORDER BY ms_played ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2
    FROM with_parent
    WINDOW w_all_by_played_at_ties AS (ORDER BY played_at)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        listener_country,
        track_is_explicit,
        ms_played_cume_dist,
        played_at_rank,
        ms_played_quintile,
        played_at_dense_rank,
        rows_in_track_is_explicit,
        second_ms_played,
        first_known_ms_played,
        ms_played_avg_2,
        ROUND(ms_played_cume_dist * 0.9, 2) AS ms_played_cume_dist_adj,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        SUBSTRING(TRIM(device_clean), 1, 3) AS device_clean_prefix
    FROM ranked
    WHERE max_ms_played_so_far > 1603000
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY played_at_rank) AS rows_in_played_at_rank,
        MAX(first_known_ms_played) OVER (PARTITION BY played_at_dense_rank ORDER BY ms_played_avg_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS first_known_ms_played_max_7,
        MAX(days_since_played_at) OVER w_played_at_rank AS played_at_rank_max_days_since_played_at
    FROM scored
    WINDOW w_played_at_rank AS (PARTITION BY played_at_rank)
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    track_is_explicit,
    ms_played_cume_dist,
    played_at_rank,
    ms_played_quintile,
    played_at_dense_rank,
    rows_in_track_is_explicit,
    second_ms_played,
    first_known_ms_played,
    ms_played_cume_dist_adj,
    device_clean_prefix,
    rows_in_played_at_rank,
    first_known_ms_played_max_7,
    CASE WHEN listener_country IN ('KR', 'TH') THEN 'group_a' WHEN listener_country = 'UK' THEN 'group_b' ELSE 'other' END AS listener_country_group
FROM windowed
ORDER BY played_at_rank, rows_in_track_is_explicit DESC NULLS FIRST
