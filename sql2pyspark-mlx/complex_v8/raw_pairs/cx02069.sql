WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-02-06 15:00:00'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        played_at_label,
        played_at_plus_1d,
        MIN(ms_played) OVER (PARTITION BY is_skipped, MONTH(played_at_plus_1d)) AS is_skipped_played_at_plus_1d_month_min_ms_played,
        COUNT(*) OVER (PARTITION BY is_skipped, MONTH(played_at_plus_1d)) AS rows_in_is_skipped_played_at_plus_1d_month,
        MIN(ms_played) OVER (PARTITION BY device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY is_skipped, MONTH(played_at_plus_1d)), 2) AS ms_played_vs_is_skipped_played_at_plus_1d_month_,
        SUM(ms_played) OVER (PARTITION BY device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played
    FROM valid
),
source_stats AS (
    -- Median ms_played_vs_is_skipped_played_at_plus_1d_month_ per source
    SELECT source, PERCENTILE_APPROX(ms_played_vs_is_skipped_played_at_plus_1d_month_, 0.5) AS median_ms_played_vs_is_skipped_played_at_plus_1d
    FROM ranked
    GROUP BY source
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ms_played_vs_is_skipped_played_at_plus_1d,
        ROUND(s.ms_played_vs_is_skipped_played_at_plus_1d_month_ - g.median_ms_played_vs_is_skipped_played_at_plus_1d, 2) AS ms_played_vs_is_skipped_played_at_plus_1d_month__2
    FROM ranked s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played_vs_is_skipped_played_at_plus_1d_month_ >= g.median_ms_played_vs_is_skipped_played_at_plus_1d
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(track_id) OVER w_listener_id) AS distinct_track_id_in_listener_id,
        ROUND(running_ms_played - AVG(running_ms_played) OVER w_listener_id, 2) AS running_ms_played_vs_listener_id_avg,
        ROUND(AVG(min_ms_played_so_far) OVER w_listener_id, 2) AS listener_id_avg_min_ms_played_so_far,
        RANK() OVER (ORDER BY rows_in_is_skipped_played_at_plus_1d_month NULLS LAST) AS rows_in_is_skipped_played_at_plus_1d_month_rank
    FROM above_average
    WINDOW w_listener_id AS (PARTITION BY listener_id)
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY rows_in_is_skipped_played_at_plus_1d_month_rank, source ORDER BY ms_played_vs_is_skipped_played_at_plus_1d_month_ DESC, id), 3) AS ms_played_vs_is_skipped_played_at_plus_1d_month__3,
        ROUND(running_ms_played - AVG(running_ms_played) OVER (PARTITION BY track_id, source), 2) AS running_ms_played_vs_track_id_source_avg
    FROM sequenced
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.played_at_label,
        s.is_skipped_played_at_plus_1d_month_min_ms_played,
        s.min_ms_played_so_far,
        s.ms_played_vs_is_skipped_played_at_plus_1d_month_,
        s.running_ms_played,
        s.median_ms_played_vs_is_skipped_played_at_plus_1d,
        s.ms_played_vs_is_skipped_played_at_plus_1d_month__2,
        s.rows_in_is_skipped_played_at_plus_1d_month_rank,
        s.ms_played_vs_is_skipped_played_at_plus_1d_month__3,
        s.running_ms_played_vs_track_id_source_avg,
        t.artist_id AS track_artist_id
    FROM sequenced_2 s
    JOIN tracks t ON t.id = s.track_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        source,
        played_at_label,
        is_skipped_played_at_plus_1d_month_min_ms_played,
        min_ms_played_so_far,
        running_ms_played,
        median_ms_played_vs_is_skipped_played_at_plus_1d,
        ms_played_vs_is_skipped_played_at_plus_1d_month__2,
        ms_played_vs_is_skipped_played_at_plus_1d_month__3,
        running_ms_played_vs_track_id_source_avg,
        track_artist_id,
        MAX(running_ms_played) OVER (PARTITION BY source ORDER BY running_ms_played, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_ms_played_max_3,
        ROUND(AVG(running_ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played_vs_is_skipped_played_at_plus_1d_month__3 DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS running_ms_played_avg_3,
        ROUND(running_ms_played - AVG(running_ms_played) OVER (PARTITION BY source, track_artist_id), 2) AS running_ms_played_vs_source_track_artist_id_avg
    FROM with_parent
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    source,
    played_at_label,
    is_skipped_played_at_plus_1d_month_min_ms_played,
    running_ms_played,
    median_ms_played_vs_is_skipped_played_at_plus_1d,
    running_ms_played_vs_track_id_source_avg,
    track_artist_id,
    running_ms_played_max_3,
    running_ms_played_avg_3
FROM with_trends
