WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE track_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, is_skipped ORDER BY ms_played, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY is_skipped), 2) AS ms_played_vs_is_skipped_avg,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY is_skipped)) AS distinct_source_in_is_skipped
    FROM cleaned_plays
),
combined_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.ms_played_avg_7,
        s.distinct_source_in_is_skipped,
        l.birth_year AS listener_birth_year
    FROM ranked s
    JOIN listeners l ON l.id = s.listener_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, distinct_source_in_is_skipped, ms_played, listener_birth_year
    FROM combined_details
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric IN (distinct_source_in_is_skipped, ms_played, listener_birth_year)
) unp
