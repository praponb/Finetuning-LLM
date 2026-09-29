WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
    WHERE course_id IS NOT NULL
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM prepared_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM sessionized s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
),
deduped AS (
    -- Keep the latest row per grade_status
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        prev_enrolled_at,
        new_session_flag,
        burst_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id DESC) AS row_num
        FROM with_activity
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    student_id,
    term,
    score,
    grade_status,
    enrolled_at,
    burst_id,
    LENGTH(TRIM(term)) AS term_len,
    LOWER(TRIM(term)) AS term_clean
FROM deduped
WHERE course_id IS NOT NULL
ORDER BY grade_status, term_clean NULLS LAST
