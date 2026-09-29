WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        prev_enrolled_at,
        burst_id,
        COUNT(*) OVER (PARTITION BY term, student_id) AS rows_in_term_student_id,
        NTILE(10) OVER (PARTITION BY grade_status ORDER BY starts_session DESC, id) AS starts_session_decile,
        ROUND((UNIX_TIMESTAMP(prev_enrolled_at) - UNIX_TIMESTAMP(LAG(prev_enrolled_at) OVER (PARTITION BY term ORDER BY prev_enrolled_at, id))) / 60, 1) AS minutes_since_prev_prev_enrolled_at,
        MIN(starts_session) OVER w_term_student_id AS term_student_id_min_starts_session,
        FIRST_VALUE(prev_enrolled_at) IGNORE NULLS OVER (PARTITION BY term ORDER BY prev_enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_enrolled_at,
        starts_session - LAG(starts_session) OVER (PARTITION BY grade_status ORDER BY starts_session DESC, id) AS starts_session_change
    FROM sessionized
    WINDOW w_term_student_id AS (PARTITION BY term, student_id)
)
-- Subtotals with GROUPING SETS
SELECT
    student_id,
    term_student_id_min_starts_session,
    COUNT(DISTINCT course_id) AS distinct_course_ids
FROM with_trends
GROUP BY GROUPING SETS ((student_id, term_student_id_min_starts_session), (student_id), (term_student_id_min_starts_session), ())
ORDER BY student_id NULLS LAST, term_student_id_min_starts_session NULLS LAST
