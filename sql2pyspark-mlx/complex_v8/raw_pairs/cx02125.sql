WITH base AS (
    -- Scope the enrollments rows
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
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM sessionized s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id >= 8
              AND e.grade_status IS NOT NULL
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        is_new_session,
        session_id,
        FIRST_VALUE(prev_enrolled_at) OVER (ORDER BY is_new_session NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prev_enrolled_at,
        COUNT(*) OVER (PARTITION BY session_id, is_new_session) AS rows_in_session_id_is_new_session,
        ROUND(session_id - AVG(session_id) OVER (PARTITION BY session_id, is_new_session), 2) AS session_id_vs_session_id_is_new_session_avg,
        SUM(score) OVER (ORDER BY is_new_session NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_sum_2
    FROM without_matches
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT first_prev_enrolled_at, grade_status, score
    FROM metrics
) src
PIVOT (
    AVG(score)
    FOR grade_status IN ('WITHDRAWN', 'FAIL', 'PASS', 'INCOMPLETE')
)
ORDER BY first_prev_enrolled_at
