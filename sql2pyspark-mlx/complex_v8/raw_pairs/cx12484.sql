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
        source
    FROM plays
    WHERE source <> 'radio'
      AND ms_played <= 236512
),
source_stats AS (
    -- Median ms_played per source
    SELECT source, PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played
    FROM recent_plays
    GROUP BY source
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ms_played,
        ROUND(s.ms_played - g.median_ms_played, 2) AS ms_played_minus_median
    FROM recent_plays s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played >= g.median_ms_played
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        TO_DATE(played_at) AS played_at_date,
        ROUND(median_ms_played * 1.5, 2) AS median_ms_played_adj,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM compared
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.ms_played_minus_median,
        s.median_ms_played_adj,
        s.is_skipped_label,
        p.id AS play_id,
        p.source AS play_source,
        p.listener_id AS play_listener_id,
        p.is_skipped AS play_is_skipped
    FROM enriched s
    LEFT JOIN plays p ON p.track_id = s.track_id
),
summary AS (
    -- Group-level totals
    SELECT
        is_skipped,
        is_skipped_label,
        play_listener_id,
        MAX(ms_played) AS max_ms_played,
        SUM(CASE WHEN play_source = 'playlist' THEN median_ms_played_adj ELSE 0 END) AS median_ms_played_adj_play_source_playlist,
        ROUND(SUM(median_ms_played_adj) / NULLIF(SUM(ms_played), 0), 3) AS median_ms_played_adj_to_ms_played_ratio,
        ROUND(STDDEV(ms_played_minus_median), 2) AS stddev_ms_played_minus_median,
        SUM(median_ms_played_adj) FILTER (WHERE play_is_skipped = TRUE) AS median_ms_played_adj_play_is_skipped_true,
        COUNT(*) AS n_rows
    FROM detail_rows_plays
    GROUP BY 1, 2, 3
),
metrics AS (
    -- Previous / next values per key
    SELECT
        is_skipped,
        is_skipped_label,
        play_listener_id,
        median_ms_played_adj_play_source_playlist,
        median_ms_played_adj_to_ms_played_ratio,
        stddev_ms_played_minus_median,
        median_ms_played_adj_play_is_skipped_true,
        n_rows,
        SIZE(COLLECT_SET(play_listener_id) OVER w_is_skipped_label) AS distinct_play_listener_id_in_is_skipped_label,
        LAG(max_ms_played, 2) OVER w_n_rows_by_median_ms_played_a AS prev2_max_ms_played,
        ROUND(n_rows / NULLIF(SUM(n_rows) OVER (PARTITION BY is_skipped_label), 0), 4) AS n_rows_share_of_is_skipped_label
    FROM summary
    WINDOW w_is_skipped_label AS (PARTITION BY is_skipped_label), w_n_rows_by_median_ms_played_a AS (PARTITION BY n_rows ORDER BY median_ms_played_adj_play_is_skipped_true DESC NULLS FIRST, is_skipped, is_skipped_label, play_listener_id)
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.play_listener_id
              AND p.played_at >= TIMESTAMP '2025-02-06 09:00:00'
        )
)
SELECT *
FROM (
    SELECT play_listener_id, is_skipped_label, prev2_max_ms_played
    FROM qualified
) src
PIVOT (
    MIN(prev2_max_ms_played)
    FOR is_skipped_label IN ('yes', 'no')
)
