WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source
    FROM plays
    WHERE source = 'search'
      AND ms_played <= 291723
),
combined_details AS (
    -- Join tracks
    SELECT s.*, t.is_explicit AS track_is_explicit, t.bpm AS track_bpm
    FROM valid s
    LEFT JOIN tracks t ON t.id = s.track_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER w_source_by_played_at AS played_at_quartile,
        MIN(track_bpm) OVER (PARTITION BY source) AS source_min_track_bpm,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER (PARTITION BY listener_id), 0), 4) AS track_bpm_share_of_listener_id,
        SUM(track_bpm) OVER (PARTITION BY listener_id, is_skipped) AS listener_id_is_skipped_sum_track_bpm,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER w_is_skipped_by_played_at)) / 60, 1) AS minutes_since_prev_played_at,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY listener_id)) AS distinct_source_in_listener_id,
        NTH_VALUE(track_bpm, 2) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_bpm,
        NTILE(4) OVER (ORDER BY played_at DESC, id) AS played_at_quartile_2,
        COUNT(*) OVER w_listener_id AS rows_in_listener_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY track_bpm, id), 3) AS track_bpm_percent_rank,
        ROUND(AVG(track_bpm) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS track_bpm_avg_7,
        SUM(track_bpm) OVER (PARTITION BY source) AS source_sum_track_bpm
    FROM combined_details
    WINDOW w_source_by_played_at AS (PARTITION BY source ORDER BY played_at, id), w_source AS (PARTITION BY source), w_is_skipped_by_played_at AS (PARTITION BY is_skipped ORDER BY played_at ASC NULLS LAST, id), w_listener_id AS (PARTITION BY listener_id)
),
per_group AS (
    -- Group-level totals
    SELECT
        distinct_source_in_listener_id,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        ROUND(100 * COUNT(CASE WHEN is_skipped = TRUE THEN 1 END) / COUNT(*), 1) AS pct_is_skipped_true,
        SUM(played_at_quartile) AS total_played_at_quartile,
        MIN(track_bpm_percent_rank) AS min_track_bpm_percent_rank
    FROM sequenced
    GROUP BY distinct_source_in_listener_id
    HAVING SUM(second_track_bpm) > 76
)
-- Final projection
SELECT
    distinct_source_in_listener_id,
    source_list,
    total_played_at_quartile,
    min_track_bpm_percent_rank
FROM per_group
ORDER BY total_played_at_quartile, min_track_bpm_percent_rank DESC, distinct_source_in_listener_id
LIMIT 5
