WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        CASE WHEN device IN ('desktop', 'speaker') THEN 'group_a' WHEN device = 'car' THEN 'group_b' ELSE 'other' END AS device_group,
        ABS(ms_played - 245877) AS ms_played_distance
    FROM plays
    WHERE (track_id >= 17 OR listener_id >= 12)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        source,
        listener_id,
        ROUND(100 * COUNT(CASE WHEN source NOT IN ('search', 'playlist') THEN 1 END) / COUNT(*), 1) AS pct_source_not_in_search_playlis,
        SUM(ms_played) AS total_ms_played
    FROM base
    GROUP BY 1, 2
    HAVING AVG(ms_played_distance) > 80000
),
derived AS (
    -- Derive helper columns
    SELECT
        source,
        listener_id,
        pct_source_not_in_search_playlis,
        total_ms_played,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        FLOOR(total_ms_played / 1000000) * 1000000 AS total_ms_played_bucket
    FROM stats
),
with_details AS (
    -- Join listeners
    SELECT
        s.*,
        l.username AS listener_username,
        l.birth_year AS listener_birth_year,
        l.country AS listener_country
    FROM derived s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.country <> 'UK'
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.source,
        s.listener_id,
        s.total_ms_played,
        s.source_prefix,
        s.total_ms_played_bucket,
        s.listener_country,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at,
        p.source AS play_source
    FROM with_details s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
),
listener_country_stats AS (
    -- Median total_ms_played per listener_country
    SELECT listener_country, PERCENTILE_APPROX(total_ms_played, 0.5) AS median_total_ms_played
    FROM with_children_plays
    GROUP BY listener_country
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_total_ms_played,
        ROUND(s.total_ms_played - g.median_total_ms_played, 2) AS total_ms_played_minus_median
    FROM with_children_plays s
    JOIN listener_country_stats g ON g.listener_country = s.listener_country
    WHERE s.total_ms_played >= g.median_total_ms_played
),
tagged AS (
    -- Derive helper columns
    SELECT
        source,
        listener_id,
        total_ms_played,
        total_ms_played_bucket,
        listener_country,
        play_id,
        play_is_skipped,
        play_played_at,
        play_source,
        median_total_ms_played,
        total_ms_played + median_total_ms_played AS total_ms_played_plus_median_total_ms_played,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        ABS(total_ms_played_bucket - 843250) AS total_ms_played_bucket_distance
    FROM compared
)
-- Final projection
SELECT
    source,
    listener_id,
    total_ms_played_bucket,
    listener_country,
    play_is_skipped,
    play_played_at,
    play_source,
    median_total_ms_played,
    total_ms_played_bucket_distance
FROM tagged
WHERE source_slug IS NOT NULL
