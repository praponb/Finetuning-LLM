WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        UPPER(TRIM(source)) AS source_clean,
        DATE_TRUNC('QUARTER', played_at) AS played_at_quarter
    FROM plays
    WHERE is_skipped = TRUE
),
without_matches AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND LENGTH(TRIM(l.username)) >= 8
              AND l.country <> 'UK'
        )
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        played_at,
        device,
        is_skipped,
        source_clean,
        played_at_quarter,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY track_id, is_skipped ORDER BY played_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        LAG(played_at, 3) OVER (PARTITION BY device ORDER BY played_at_quarter DESC, id) AS prev3_played_at
    FROM without_matches
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.first_known_played_at,
        s.prev3_played_at,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.source AS play_source,
        p.played_at AS play_played_at
    FROM sequenced s
    JOIN plays p ON p.listener_id = s.listener_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at
    FROM detail_rows_plays s
    LEFT JOIN play_stats p ON p.track_id = s.play_track_id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        device,
        play_track_id,
        DATE_TRUNC('MONTH', prev3_played_at) AS prev3_played_at_month,
        MAX(play_played_at) AS last_play_played_at,
        COUNT(DISTINCT play_source) AS distinct_play_sources
    FROM profiled
    GROUP BY 1, 2, 3
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        device,
        play_track_id,
        prev3_played_at_month,
        last_play_played_at,
        distinct_play_sources,
        LAG(last_play_played_at) OVER w_distinct_play_sources_by_las AS prev_last_play_played_at,
        ROUND(distinct_play_sources - AVG(distinct_play_sources) OVER w_distinct_play_sources_prev3, 2) AS distinct_play_sources_vs_distinct_play_sources_p,
        ROUND(AVG(distinct_play_sources) OVER (PARTITION BY play_track_id ORDER BY distinct_play_sources ASC, device, play_track_id, prev3_played_at_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS distinct_play_sources_avg_6_prior
    FROM per_group
    WINDOW w_distinct_play_sources_by_las AS (PARTITION BY distinct_play_sources ORDER BY last_play_played_at DESC NULLS FIRST, device, play_track_id, prev3_played_at_month), w_distinct_play_sources_prev3 AS (PARTITION BY distinct_play_sources, DATE_TRUNC('MONTH', prev3_played_at_month))
)
-- Final projection
SELECT
    device,
    play_track_id,
    last_play_played_at,
    prev_last_play_played_at,
    distinct_play_sources_vs_distinct_play_sources_p,
    distinct_play_sources_avg_6_prior,
    LOWER(TRIM(device)) AS device_clean
FROM sequenced_2
ORDER BY device_clean DESC NULLS FIRST, prev_last_play_played_at DESC
