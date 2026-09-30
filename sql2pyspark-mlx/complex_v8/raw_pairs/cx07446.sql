WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        DATE_TRUNC('YEAR', played_at) AS played_at_year,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        ABS(ms_played - 112786) AS ms_played_distance
    FROM plays
    WHERE track_id IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        played_at,
        is_skipped,
        played_at_year,
        ms_played_bucket,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY YEAR(played_at_year), source)) AS distinct_listener_id_in_played_at_year_year_sour,
        COUNT(*) OVER w_played_at_year_year_source AS rows_in_played_at_year_year_source,
        FIRST_VALUE(played_at_year) IGNORE NULLS OVER (PARTITION BY track_id ORDER BY played_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_year,
        ROUND(PERCENT_RANK() OVER w_track_id_by_played_at_year_t, 3) AS played_at_year_percent_rank,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY played_at_year DESC, id) AS played_at_year_dense_rank
    FROM recent_plays
    WINDOW w_played_at_year_year_source AS (PARTITION BY YEAR(played_at_year), source), w_track_id_by_played_at_year_t AS (PARTITION BY track_id ORDER BY played_at_year DESC)
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.track_id,
        s.ms_played_bucket,
        s.distinct_listener_id_in_played_at_year_year_sour,
        s.first_known_played_at_year,
        s.played_at_year_percent_rank,
        s.played_at_year_dense_rank,
        p.id AS play_id,
        p.source AS play_source,
        p.listener_id AS play_listener_id
    FROM windowed s
    LEFT JOIN plays p ON p.track_id = s.track_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(first_known_played_at_year, 'yyyy-MM') AS first_known_played_at_year_ym,
        played_at_year_dense_rank,
        play_listener_id,
        ROUND(SUM(ms_played_bucket) / NULLIF(SUM(played_at_year_dense_rank), 0), 3) AS ms_played_bucket_to_played_at_year_dense_rank_ra,
        ROUND(STDDEV(played_at_year_dense_rank), 2) AS stddev_played_at_year_dense_rank
    FROM exploded_detail
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    first_known_played_at_year_ym,
    played_at_year_dense_rank,
    play_listener_id,
    ms_played_bucket_to_played_at_year_dense_rank_ra,
    stddev_played_at_year_dense_rank
FROM summary
ORDER BY play_listener_id DESC, first_known_played_at_year_ym, played_at_year_dense_rank
LIMIT 20
