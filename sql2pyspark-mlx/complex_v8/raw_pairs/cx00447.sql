WITH filtered AS (
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
        INITCAP(TRIM(device)) AS device_clean
    FROM plays
    WHERE MONTH(played_at) IN (2, 3)
      AND device IN ('car', 'tv')
),
combined_details AS (
    -- Attach listener attributes
    SELECT
        s.*,
        l.username AS listener_username,
        l.country AS listener_country,
        t.release_date AS track_release_date,
        t.bpm AS track_bpm
    FROM filtered s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        played_at,
        ms_played,
        listener_username,
        listener_country,
        ms_played - LAG(ms_played) OVER (PARTITION BY is_skipped ORDER BY played_at ASC, id) AS ms_played_change,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        RANK() OVER (ORDER BY ms_played ASC NULLS LAST) AS ms_played_rank,
        ROUND(track_bpm - AVG(track_bpm) OVER w_track_id, 2) AS track_bpm_vs_track_id_avg,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (ORDER BY ms_played ASC NULLS LAST, id))) / 60, 1) AS minutes_since_prev_played_at,
        LAST_VALUE(track_release_date) OVER (PARTITION BY track_id, listener_id ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_track_release_date,
        DENSE_RANK() OVER (PARTITION BY is_skipped ORDER BY played_at ASC, id) AS played_at_dense_rank,
        ROUND(AVG(track_bpm) OVER w_track_id, 2) AS track_id_avg_track_bpm,
        DENSE_RANK() OVER w_track_id_listener_id_by_trac_2 AS track_bpm_dense_rank,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped ORDER BY played_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_3,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER (PARTITION BY track_id), 0), 4) AS track_bpm_share_of_track_id,
        MAX(ms_played) OVER (PARTITION BY track_id, listener_id ORDER BY track_bpm, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_max_2,
        LAG(ms_played, 3) OVER (PARTITION BY track_id, listener_id ORDER BY track_bpm, id) AS prev3_ms_played,
        ROUND(PERCENT_RANK() OVER w_is_skipped_by_played_at, 3) AS played_at_percent_rank
    FROM combined_details
    WINDOW w_track_id AS (PARTITION BY track_id), w_track_id_listener_id_by_trac_2 AS (PARTITION BY track_id, listener_id ORDER BY track_bpm), w_is_skipped_by_played_at AS (PARTITION BY is_skipped ORDER BY played_at ASC, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(track_id_avg_track_bpm, played_at_dense_rank) AS greatest_track_id_avg_track_bpm_played_at_dense_,
        GREATEST(rows_in_track_id, prev3_ms_played) AS greatest_rows_in_track_id_prev3_ms_played,
        GREATEST(prev3_ms_played, track_id_avg_track_bpm) AS greatest_prev3_ms_played_track_id_avg_track_bpm
    FROM sequenced
    WHERE track_bpm_share_of_track_id BETWEEN 0.1 AND 0.9
),
stats AS (
    -- Group-level totals
    SELECT
        played_at_dense_rank,
        track_bpm_dense_rank,
        CASE WHEN ms_played >= 245877 THEN 'large' WHEN ms_played >= 166517 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        SUM(CASE WHEN UPPER(TRIM(listener_username)) LIKE 'G%' THEN 1 ELSE 0 END) AS n_upper_trim_listener_username,
        ARRAY_SORT(COLLECT_SET(listener_username)) AS listener_username_list,
        COUNT(*) FILTER (WHERE listener_country IS NOT NULL) AS n_listener_country_is_not_null
    FROM tagged
    WHERE ms_played_avg_3 <= 242000.0
    GROUP BY played_at_dense_rank, track_bpm_dense_rank, CASE WHEN ms_played >= 245877 THEN 'large' WHEN ms_played >= 166517 THEN 'medium' ELSE 'small' END
)
-- Final projection
SELECT
    played_at_dense_rank,
    track_bpm_dense_rank,
    ms_played_tier,
    n_upper_trim_listener_username,
    listener_username_list,
    n_listener_country_is_not_null,
    COALESCE(played_at_dense_rank, 0) AS played_at_dense_rank_filled,
    LEAST(played_at_dense_rank, track_bpm_dense_rank) AS least_played_at_dense_rank_track_bpm_dense_rank
FROM stats
ORDER BY n_listener_country_is_not_null NULLS LAST, played_at_dense_rank, track_bpm_dense_rank, ms_played_tier
LIMIT 10
