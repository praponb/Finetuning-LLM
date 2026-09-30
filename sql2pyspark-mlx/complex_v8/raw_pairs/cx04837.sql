WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2024T2', '2024T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(term)) AS term_len,
        starts_session - session_id AS starts_session_minus_session_id
    FROM sessionized
    WHERE starts_session BETWEEN 0 AND 1
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    prev_enrolled_at,
    starts_session,
    session_id,
    term_len,
    TO_DATE(prev_enrolled_at) AS prev_enrolled_at_date
FROM scored
ORDER BY id, prev_enrolled_at NULLS LAST
