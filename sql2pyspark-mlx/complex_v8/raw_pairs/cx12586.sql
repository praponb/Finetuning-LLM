WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CASE WHEN bpm >= 155 THEN 'large' WHEN bpm >= 82 THEN 'medium' ELSE 'small' END AS bpm_tier,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at_2
    FROM filtered s
),
play_stats_2 AS (
    -- Per-track plays metrics
    SELECT track_id, MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at
    FROM profiled s
    LEFT JOIN play_stats_2 p ON p.track_id = s.id
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.moods,
        s.bpm_tier,
        s.release_date_label,
        s.title_slug,
        s.first_played_at_2,
        s.last_played_at,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played,
        p.device AS play_device,
        p.source AS play_source
    FROM summarized_rows s
    JOIN plays p ON p.track_id = s.id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        moods,
        bpm_tier,
        title_slug,
        first_played_at_2,
        last_played_at,
        play_id,
        play_is_skipped,
        play_source,
        LAST_VALUE(last_played_at) OVER (PARTITION BY play_source ORDER BY play_ms_played, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_played_at,
        SIZE(COLLECT_SET(bpm_tier) OVER (PARTITION BY play_source)) AS distinct_bpm_tier_in_play_source,
        ROUND((UNIX_TIMESTAMP(first_played_at_2) - UNIX_TIMESTAMP(LAG(first_played_at_2) OVER (PARTITION BY YEAR(last_played_at) ORDER BY first_played_at_2, id, play_id))) / 60, 1) AS minutes_since_prev_first_played_at_2,
        LAST_VALUE(play_ms_played) IGNORE NULLS OVER (PARTITION BY YEAR(last_played_at) ORDER BY first_played_at_2, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_play_ms_played,
        SUM(play_ms_played) OVER (ORDER BY play_ms_played DESC NULLS FIRST, id, play_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS play_ms_played_sum_1_prior
    FROM exploded_detail_plays
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, play_id, metric_name, metric_value
FROM (
    SELECT id, play_id, play_ms_played_sum_1_prior, last_known_play_ms_played
    FROM with_windows
) src
UNPIVOT (
    metric_value FOR metric_name IN (play_ms_played_sum_1_prior, last_known_play_ms_played)
) unp
