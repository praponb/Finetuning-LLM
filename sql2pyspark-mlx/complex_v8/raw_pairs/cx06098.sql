WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE ms_played > 108979
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        device_slug,
        LOWER(REGEXP_REPLACE(TRIM(device_slug), ' ', '_')) AS device_slug_slug
    FROM scoped
    WHERE device_slug IS NOT NULL
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM scored s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE HOUR(p.played_at) BETWEEN 8 AND 13
        )
),
with_parent_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.device_slug_slug,
        l.username AS listener_username
    FROM without_matches s
    JOIN listeners l ON l.id = s.listener_id
    WHERE UPPER(TRIM(l.username)) LIKE 'E%'
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(listener_username, '#', CAST(id AS STRING)) AS listener_username_ref,
        CONCAT(listener_username, '#', CAST(track_id AS STRING)) AS listener_username_ref_2,
        SUBSTRING(TRIM(listener_username), 1, 2) AS listener_username_prefix
    FROM with_parent_listener
),
per_group AS (
    -- Group-level totals
    SELECT
        track_id,
        ARRAY_SORT(COLLECT_SET(listener_username)) AS listener_username_list,
        SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true,
        COUNT(*) FILTER (WHERE is_skipped = TRUE) AS n_is_skipped_true_2,
        COUNT(*) FILTER (WHERE is_skipped = FALSE) AS n_is_skipped_false,
        COUNT(DISTINCT track_id) AS distinct_track_ids
    FROM labeled
    GROUP BY track_id
)
SELECT
    track_id,
    n_is_skipped_true_2,
    n_is_skipped_false,
    distinct_track_ids,
    n_is_skipped_true - n_is_skipped_true_2 AS n_is_skipped_true_minus_n_is_skipped_true_2
FROM per_group
