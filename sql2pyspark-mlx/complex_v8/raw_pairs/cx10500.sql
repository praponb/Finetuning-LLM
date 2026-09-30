WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        term,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY term, session_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        term,
        session_id,
        event_count,
        session_score,
        duration_min,
        ABS(session_id - 3) AS session_id_distance,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        CASE WHEN term IN ('2025T2', '2024T1') THEN 'group_a' WHEN term = '2024T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM per_session
)
SELECT
    term,
    event_count,
    duration_min,
    session_id_distance,
    term_or_unknown,
    term_group,
    LOWER(REGEXP_REPLACE(TRIM(term_or_unknown), ' ', '_')) AS term_or_unknown_slug,
    UPPER(TRIM(term_or_unknown)) AS term_or_unknown_clean
FROM annotated
WHERE term IN ('2024T1', '2025T1', '2024T2')
ORDER BY duration_min DESC, term_or_unknown_clean DESC
