WITH cleaned_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN source IN ('radio', 'playlist') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        device,
        source_group,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_listener_id, 0), 4) AS ms_played_share_of_listener_id,
        MIN(ms_played) OVER (PARTITION BY listener_id) AS listener_id_min_ms_played,
        SUM(ms_played) OVER (PARTITION BY MONTH(played_at), source_group ORDER BY ms_played NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ms_played_sum_3_prior,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY listener_id)) AS distinct_device_in_listener_id,
        COUNT(*) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(ms_played) OVER w_listener_id AS listener_id_sum_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id, source_group), 2) AS ms_played_vs_track_id_source_group_avg,
        DENSE_RANK() OVER (ORDER BY ms_played DESC, id) AS ms_played_dense_rank,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS listener_id_avg_ms_played,
        LEAD(played_at) OVER w_all_by_ms_played AS next_played_at,
        MIN(ms_played) OVER (PARTITION BY track_id, source_group) AS track_id_source_group_min_ms_played,
        NTH_VALUE(played_at, 2) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at
    FROM cleaned_plays
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_all_by_ms_played AS (ORDER BY ms_played DESC, id)
),
totals AS (
    -- Group-level totals
    SELECT
        YEAR(second_played_at) AS second_played_at_year,
        distinct_device_in_listener_id,
        device,
        COUNT(*) FILTER (WHERE device IN ('desktop', 'speaker', 'phone')) AS n_device_in_desktop_speaker_ph,
        COUNT(*) AS n_rows,
        ROUND(SUM(ms_played_dense_rank) / NULLIF(SUM(track_id_source_group_min_ms_played), 0), 3) AS ms_played_dense_rank_to_track_id_source_group_mi,
        SUM(CASE WHEN device IN ('phone', 'desktop') THEN distinct_device_in_listener_id ELSE 0 END) AS distinct_device_in_listener_id_device_in_phone_d,
        ROUND(SUM(listener_id_min_ms_played) / NULLIF(SUM(distinct_device_in_listener_id), 0), 3) AS listener_id_min_ms_played_to_distinct_device_in_
    FROM sequenced
    GROUP BY YEAR(second_played_at), distinct_device_in_listener_id, device
    HAVING MAX(ms_played_dense_rank) > 2
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        second_played_at_year,
        distinct_device_in_listener_id,
        device,
        n_device_in_desktop_speaker_ph,
        n_rows,
        distinct_device_in_listener_id_device_in_phone_d,
        listener_id_min_ms_played_to_distinct_device_in_,
        MAX(n_rows) OVER (ORDER BY distinct_device_in_listener_id_device_in_phone_d ASC, second_played_at_year, distinct_device_in_listener_id, device ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS n_rows_max_3,
        ROW_NUMBER() OVER w_all_by_distinct_device_in_li_2 AS distinct_device_in_listener_id_device_in_phone_d_2
    FROM totals
    WINDOW w_all_by_distinct_device_in_li_2 AS (ORDER BY distinct_device_in_listener_id_device_in_phone_d ASC, second_played_at_year, distinct_device_in_listener_id, device)
)
-- Final projection
SELECT
    second_played_at_year,
    distinct_device_in_listener_id,
    n_device_in_desktop_speaker_ph,
    n_rows,
    distinct_device_in_listener_id_device_in_phone_d,
    listener_id_min_ms_played_to_distinct_device_in_,
    distinct_device_in_listener_id_device_in_phone_d_2,
    LENGTH(TRIM(device)) AS device_len
FROM sequenced_2
ORDER BY device_len, listener_id_min_ms_played_to_distinct_device_in_ DESC
