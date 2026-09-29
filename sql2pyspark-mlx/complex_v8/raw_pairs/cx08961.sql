WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 0.25, 2) AS score_adj
    FROM enrollments
    WHERE grade_status = 'INCOMPLETE'
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        score,
        is_new_session,
        session_no,
        ROUND(session_no - AVG(session_no) OVER w_grade_status_session_no, 2) AS session_no_vs_grade_status_session_no_avg,
        DENSE_RANK() OVER (ORDER BY enrolled_at DESC) AS enrolled_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status, session_no ORDER BY prev_enrolled_at ASC, id), 3) AS prev_enrolled_at_cume_dist,
        ROW_NUMBER() OVER w_session_no_by_session_no AS rn
    FROM sessionized
    WINDOW w_grade_status_session_no AS (PARTITION BY grade_status, session_no), w_session_no_by_session_no AS (PARTITION BY session_no ORDER BY session_no ASC, id)
)
-- Subtotals with GROUPING SETS
SELECT
    student_id,
    is_new_session,
    course_id,
    COUNT(*) AS row_count,
    COUNT(DISTINCT course_id) AS distinct_course_ids,
    PERCENTILE_APPROX(score, 0.5) AS median_score
FROM windowed
GROUP BY GROUPING SETS ((student_id, is_new_session, course_id), (student_id), (is_new_session), ())
ORDER BY student_id NULLS LAST, is_new_session NULLS LAST, course_id NULLS LAST
