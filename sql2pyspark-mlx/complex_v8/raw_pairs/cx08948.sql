WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE NOT is_skipped
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        l.country AS listener_country,
        l.plan AS listener_plan
    FROM scoped_plays s
    JOIN listeners l ON l.id = s.listener_id AND l.plan <> 'family'
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        UPPER(TRIM(source)) AS source_clean,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM with_details
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(is_skipped_label)) AS is_skipped_label_len,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        COALESCE(listener_plan, 'UNKNOWN') AS listener_plan_or_unknown
    FROM tagged
),
grouped AS (
    -- Summarise per group
    SELECT
        is_skipped,
        source,
        SUM(CASE WHEN listener_plan = 'student' THEN 1 ELSE 0 END) AS n_listener_plan_student,
        COUNT(*) AS cnt,
        COUNT(DISTINCT listener_country) AS distinct_listener_countrys,
        SUM(is_skipped_flag) AS total_is_skipped_flag,
        SUM(is_skipped_label_len) AS total_is_skipped_label_len
    FROM derived
    GROUP BY is_skipped, source
)
-- Final projection
SELECT
    is_skipped,
    source,
    cnt,
    total_is_skipped_flag,
    total_is_skipped_label_len,
    UPPER(TRIM(source)) AS source_clean
FROM grouped
ORDER BY source_clean DESC, total_is_skipped_label_len DESC
