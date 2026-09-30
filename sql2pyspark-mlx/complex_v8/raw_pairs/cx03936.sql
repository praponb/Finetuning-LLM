WITH cleaned_plays AS (
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
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket_2
    FROM plays
    WHERE source = 'search'
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        played_at,
        device,
        source,
        ms_played_bucket,
        TO_DATE(played_at) AS played_at_date,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM cleaned_plays
),
unique_rows AS (
    -- Keep the latest row per device
    SELECT
        id,
        listener_id,
        played_at,
        device,
        source,
        ms_played_bucket,
        played_at_date,
        source_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device ORDER BY ms_played_bucket DESC, id DESC) AS recency_rank
        FROM scored
    ) ranked
    WHERE recency_rank = 1
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS max_play_ms_played
    FROM unique_rows s
),
rollup_base AS (
    -- Group-level totals
    SELECT
        device,
        listener_id,
        source,
        ROUND(SUM(max_play_ms_played) / NULLIF(SUM(ms_played_bucket), 0), 3) AS max_play_ms_played_to_ms_played_bucket_ratio,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        COUNT(DISTINCT source) AS distinct_sources
    FROM with_stats
    GROUP BY device, listener_id, source
    HAVING MAX(max_play_ms_played) > 123500
),
with_parent_listener AS (
    -- Attach listener attributes
    SELECT
        s.device,
        s.listener_id,
        s.source,
        s.max_play_ms_played_to_ms_played_bucket_ratio,
        s.source_list,
        s.distinct_sources,
        l.birth_year AS listener_birth_year
    FROM rollup_base s
    JOIN listeners l ON l.id = s.listener_id AND l.country <> 'JP'
)
-- Final projection
SELECT
    device,
    listener_id,
    max_play_ms_played_to_ms_played_bucket_ratio,
    source_list,
    distinct_sources,
    CONCAT(device, '#', CAST(listener_id AS STRING)) AS device_ref,
    SUBSTRING(TRIM(source), 1, 3) AS source_prefix
FROM with_parent_listener
ORDER BY device DESC, max_play_ms_played_to_ms_played_bucket_ratio
