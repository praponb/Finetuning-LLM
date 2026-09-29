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
    WHERE track_id >= 16
),
with_details AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        t.is_explicit AS track_is_explicit,
        a.artist_name
    FROM scoped s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(played_at) OVER (PARTITION BY track_is_explicit, track_id ORDER BY ms_played ASC, id) AS next_played_at,
        COUNT(*) OVER (PARTITION BY source) AS rows_in_source,
        ROW_NUMBER() OVER (ORDER BY ms_played, id) AS ms_played_seq,
        NTH_VALUE(played_at, 2) OVER (ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        LAST_VALUE(played_at) OVER (PARTITION BY track_is_explicit, track_id ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        DENSE_RANK() OVER (ORDER BY played_at, id) AS played_at_dense_rank,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id), 2) AS ms_played_vs_track_id_avg,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (ORDER BY ms_played NULLS LAST, id))) / 60, 1) AS minutes_since_prev_played_at,
        SIZE(COLLECT_SET(track_id) OVER w_source) AS distinct_track_id_in_source,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id), 2) AS track_id_avg_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_3_prior,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_id), 0), 4) AS ms_played_share_of_track_id,
        FIRST_VALUE(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        MIN(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        RANK() OVER (PARTITION BY listener_id ORDER BY ms_played ASC) AS ms_played_rank
    FROM with_details
    WINDOW w_source AS (PARTITION BY source)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(ms_played_avg_3_prior) OVER (PARTITION BY source) AS source_sum_ms_played_avg_3_prior,
        MAX(rows_in_track_id) OVER (PARTITION BY listener_id) AS listener_id_max_rows_in_track_id,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id)) AS distinct_track_id_in_listener_id,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY source)) AS distinct_track_id_in_source_2,
        MAX(distinct_track_id_in_source) OVER (PARTITION BY ms_played_rank, listener_id ORDER BY second_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_distinct_track_id_in_source_so_far,
        NTILE(4) OVER (ORDER BY distinct_track_id_in_source, id) AS distinct_track_id_in_source_quartile
    FROM ranked
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    source,
    track_is_explicit,
    artist_name,
    next_played_at,
    ms_played_seq,
    second_played_at,
    played_at_dense_rank,
    ms_played_vs_track_id_avg,
    minutes_since_prev_played_at,
    distinct_track_id_in_source,
    track_id_avg_ms_played,
    ms_played_avg_3_prior,
    ms_played_share_of_track_id,
    rows_in_track_id,
    ms_played_rank,
    source_sum_ms_played_avg_3_prior,
    distinct_track_id_in_listener_id,
    distinct_track_id_in_source_2,
    max_distinct_track_id_in_source_so_far,
    CONCAT(artist_name, '#', CAST(ms_played_rank AS STRING)) AS artist_name_ref,
    CASE WHEN source IN ('search', 'album') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group
FROM with_trends
