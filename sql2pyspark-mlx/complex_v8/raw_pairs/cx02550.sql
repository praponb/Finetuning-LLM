WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE listener_id IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        source,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY source ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        SUM(ms_played) OVER (PARTITION BY source) AS source_sum_ms_played,
        LEAD(ms_played, 2, 0) OVER w_all_by_ms_played AS next2_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY source ORDER BY ms_played DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY source ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_2,
        ROW_NUMBER() OVER (ORDER BY ms_played DESC NULLS FIRST, id) AS ms_played_seq,
        FIRST_VALUE(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        LEAD(ms_played) OVER (PARTITION BY source ORDER BY ms_played DESC, id) AS next_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_id, listener_id), 0), 4) AS ms_played_share_of_track_id_listener_id,
        ROUND(AVG(ms_played) OVER (PARTITION BY source ORDER BY ms_played DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_4
    FROM prepared
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        source,
        second_ms_played,
        source_sum_ms_played,
        next2_ms_played,
        ms_played_avg_2_prior,
        second_ms_played_2,
        first_ms_played,
        next_ms_played,
        ms_played_share_of_track_id_listener_id,
        ms_played_avg_4
    FROM windowed
    WHERE ms_played_seq <= 2
),
per_group AS (
    -- Group-level totals
    SELECT
        source,
        track_id,
        listener_id,
        MAX(ms_played) AS max_ms_played,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE source = 'playlist') AS n_source_playlist,
        COUNT(DISTINCT source) AS distinct_sources,
        COUNT(*) FILTER (WHERE source IN ('search', 'radio', 'album')) AS n_source_in_search_radio_album
    FROM first_rows
    GROUP BY source, track_id, listener_id
    HAVING COUNT(*) >= 2
)
SELECT
    source,
    track_id,
    listener_id,
    max_ms_played,
    distinct_sources,
    n_source_in_search_radio_album,
    CASE WHEN n_source_playlist >= 8 THEN 'HIGH' WHEN n_source_playlist >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS n_source_playlist_tier,
    GREATEST(distinct_sources, cnt) AS greatest_distinct_sources_cnt
FROM per_group
WHERE source NOT IN ('playlist', 'radio')
ORDER BY track_id
