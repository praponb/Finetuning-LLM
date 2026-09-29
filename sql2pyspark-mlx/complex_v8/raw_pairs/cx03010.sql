WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(device)) AS device_len,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        CASE WHEN ms_played >= 257789 THEN 'large' WHEN ms_played >= 129336 THEN 'medium' ELSE 'small' END AS ms_played_tier
    FROM plays
    WHERE listener_id IS NOT NULL
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        s.device_len,
        s.ms_played_tier,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped
    FROM cleaned s
    JOIN plays p ON p.listener_id = s.listener_id
),
track_id_stats AS (
    -- Median play_ms_played per track_id
    SELECT track_id, PERCENTILE_APPROX(play_ms_played, 0.5) AS median_play_ms_played
    FROM exploded_detail_plays
    GROUP BY track_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_play_ms_played,
        ROUND(s.play_ms_played - g.median_play_ms_played, 2) AS play_ms_played_minus_median
    FROM exploded_detail_plays s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.play_ms_played >= g.median_play_ms_played
),
joined AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.ms_played_tier,
        s.play_id,
        s.play_is_skipped,
        s.median_play_ms_played,
        s.play_ms_played_minus_median,
        l.country AS listener_country
    FROM above_average s
    LEFT JOIN listeners l ON l.id = s.listener_id AND l.birth_year >= 1970
),
deduped AS (
    -- Keep the earliest row per ms_played_tier, track_id
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        source,
        ms_played_tier,
        play_id,
        play_is_skipped,
        median_play_ms_played,
        play_ms_played_minus_median,
        listener_country
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ms_played_tier, track_id ORDER BY play_ms_played_minus_median, id, play_id) AS row_num
        FROM joined
    ) ranked
    WHERE row_num = 1
),
with_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.ms_played_tier,
        s.play_id,
        s.play_is_skipped,
        s.median_play_ms_played,
        s.play_ms_played_minus_median,
        s.listener_country,
        t.moods AS track_moods,
        t.is_explicit AS track_is_explicit,
        p.ms_played AS play_ms_played
    FROM deduped s
    JOIN tracks t ON t.id = s.track_id AND t.duration_sec <= 352
    JOIN plays p ON p.id = s.play_id
),
with_parent_play AS (
    -- Enrich with dimension data
    SELECT s.*, p.track_id AS play_track_id, p.source AS play_source
    FROM with_details_track s
    LEFT JOIN plays p ON p.id = s.play_id
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    source,
    play_id,
    play_is_skipped,
    median_play_ms_played,
    listener_country,
    track_moods,
    track_is_explicit,
    play_ms_played,
    play_track_id,
    play_source,
    COALESCE(median_play_ms_played, 0.0) AS median_play_ms_played_filled,
    LOWER(REGEXP_REPLACE(TRIM(play_source), ' ', '_')) AS play_source_slug
FROM with_parent_play
WHERE source = 'search'
ORDER BY play_id NULLS LAST
