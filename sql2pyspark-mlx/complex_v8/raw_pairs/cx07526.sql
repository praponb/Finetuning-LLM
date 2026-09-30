WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        DATE_TRUNC('QUARTER', played_at) AS played_at_quarter
    FROM plays
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        played_at_quarter,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id ORDER BY played_at NULLS LAST), 3) AS played_at_cume_dist,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY device, is_skipped)) AS distinct_listener_id_in_device_is_skipped,
        LAG(played_at, 3) OVER (PARTITION BY YEAR(played_at) ORDER BY played_at_quarter, id) AS prev3_played_at,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id ORDER BY played_at NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', played_at)), 2) AS ms_played_vs_listener_id_played_at_month_avg,
        ROUND((UNIX_TIMESTAMP(played_at_quarter) - UNIX_TIMESTAMP(LAG(played_at_quarter) OVER w_track_id_by_played_at)) / 60, 1) AS minutes_since_prev_played_at_quarter,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', played_at))) AS distinct_track_id_in_listener_id_played_at_month,
        ROW_NUMBER() OVER w_listener_id_played_at_year_b AS rn,
        RANK() OVER (PARTITION BY YEAR(played_at) ORDER BY played_at_quarter) AS played_at_quarter_rank,
        COUNT(*) OVER (PARTITION BY YEAR(played_at) ORDER BY played_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(played_at_quarter) ORDER BY played_at_quarter), 3) AS played_at_quarter_percent_rank,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', played_at)), 2) AS listener_id_played_at_month_avg_ms_played
    FROM valid_plays
    WINDOW w_track_id_by_played_at AS (PARTITION BY track_id ORDER BY played_at NULLS LAST, id), w_listener_id_played_at_year_b AS (PARTITION BY listener_id, YEAR(played_at) ORDER BY played_at, id), w_listener_id_played_at_month AS (PARTITION BY listener_id, DATE_TRUNC('MONTH', played_at))
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        played_at_quarter,
        distinct_listener_id_in_device_is_skipped,
        prev3_played_at,
        ms_played_avg_2,
        ms_played_vs_listener_id_played_at_month_avg,
        minutes_since_prev_played_at_quarter,
        distinct_track_id_in_listener_id_played_at_month,
        rn,
        played_at_quarter_rank,
        running_count,
        played_at_quarter_percent_rank,
        listener_id_played_at_month_avg_ms_played,
        CASE WHEN device IN ('tv', 'desktop') THEN 'group_a' WHEN device = 'car' THEN 'group_b' ELSE 'other' END AS device_group
    FROM ranked
    WHERE played_at_quarter_percent_rank <= 0.6
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY played_at_quarter_rank)) AS distinct_listener_id_in_played_at_quarter_rank,
        ROW_NUMBER() OVER w_distinct_track_id_in_listene AS listener_id_played_at_month_avg_ms_played_seq,
        LAST_VALUE(rn) OVER (PARTITION BY is_skipped, device_group ORDER BY played_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rn,
        NTILE(3) OVER w_is_skipped_by_distinct_track AS distinct_track_id_in_listener_id_played_at_month_2,
        COUNT(*) OVER (PARTITION BY played_at_quarter_rank, track_id) AS rows_in_played_at_quarter_rank_track_id
    FROM scored
    WINDOW w_distinct_track_id_in_listene AS (PARTITION BY distinct_track_id_in_listener_id_played_at_month, distinct_listener_id_in_device_is_skipped ORDER BY listener_id_played_at_month_avg_ms_played, id), w_is_skipped_by_distinct_track AS (PARTITION BY is_skipped ORDER BY distinct_track_id_in_listener_id_played_at_month, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    is_skipped,
    played_at_quarter,
    distinct_listener_id_in_device_is_skipped,
    prev3_played_at,
    ms_played_avg_2,
    ms_played_vs_listener_id_played_at_month_avg,
    minutes_since_prev_played_at_quarter,
    distinct_track_id_in_listener_id_played_at_month,
    rn,
    played_at_quarter_rank,
    played_at_quarter_percent_rank,
    listener_id_played_at_month_avg_ms_played,
    device_group,
    distinct_listener_id_in_played_at_quarter_rank,
    listener_id_played_at_month_avg_ms_played_seq,
    distinct_track_id_in_listener_id_played_at_month_2,
    rows_in_played_at_quarter_rank_track_id,
    DATE_FORMAT(played_at_quarter, 'yyyy-MM-dd') AS played_at_quarter_label,
    LOWER(TRIM(device_group)) AS device_group_clean
FROM with_trends
ORDER BY prev3_played_at DESC, listener_id_played_at_month_avg_ms_played_seq
