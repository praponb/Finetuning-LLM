WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE ms_played >= 52009
      AND is_skipped = FALSE
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        is_skipped,
        source,
        source_ref,
        ms_played_filled,
        ms_played - ms_played_filled AS ms_played_minus_ms_played_filled,
        ROUND(ms_played_filled / NULLIF(ms_played, 0), 3) AS ms_played_filled_per_ms_played
    FROM scoped
    WHERE source = 'radio'
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM labeled s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE TO_DATE(p.played_at) < DATE '2025-02-12'
        )
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        is_skipped,
        source_ref,
        ms_played_filled,
        ms_played_minus_ms_played_filled,
        COALESCE(ms_played_filled, 0) AS ms_played_filled_filled,
        SUBSTRING(TRIM(source_ref), 1, 2) AS source_ref_prefix
    FROM qualified
)
-- Final projection
SELECT
    id,
    is_skipped,
    source_ref,
    ms_played_filled,
    ms_played_minus_ms_played_filled,
    ms_played_filled_filled,
    source_ref_prefix
FROM tagged
ORDER BY ms_played_minus_ms_played_filled, ms_played_filled
