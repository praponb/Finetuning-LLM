WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug_2
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-31 09:00:00'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(is_skipped_flag, 2) OVER (PARTITION BY source ORDER BY is_skipped_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_is_skipped_flag,
        NTILE(10) OVER (ORDER BY played_at, id) AS played_at_decile,
        SIZE(COLLECT_SET(listener_id) OVER w_source) AS distinct_listener_id_in_source,
        SIZE(COLLECT_SET(track_id) OVER w_source) AS distinct_track_id_in_source,
        ROUND(AVG(ms_played) OVER w_source, 2) AS source_avg_ms_played
    FROM base
    WINDOW w_source AS (PARTITION BY source)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at,
        TO_DATE(played_at) AS played_at_date
    FROM sequenced
    WHERE second_is_skipped_flag <= 1
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.device IN ('car', 'phone', 'tv')
        )
),
track_id_stats AS (
    -- Median distinct_track_id_in_source per track_id
    SELECT track_id, PERCENTILE_APPROX(distinct_track_id_in_source, 0.5) AS median_distinct_track_id_in_source
    FROM filtered
    GROUP BY track_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_distinct_track_id_in_source,
        ROUND(s.distinct_track_id_in_source - g.median_distinct_track_id_in_source, 2) AS distinct_track_id_in_source_minus_median
    FROM filtered s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.distinct_track_id_in_source >= g.median_distinct_track_id_in_source
)
SELECT id, field, value
FROM (
    SELECT id, distinct_track_id_in_source_minus_median, median_distinct_track_id_in_source, source_avg_ms_played
    FROM compared
) src
UNPIVOT (
    value FOR field IN (distinct_track_id_in_source_minus_median AS distinct_track_id_in_source_minus_median_v, median_distinct_track_id_in_source AS m_median_distinct_track_id_in_source, source_avg_ms_played AS m_source_avg_ms_played)
) unp
ORDER BY id, field
