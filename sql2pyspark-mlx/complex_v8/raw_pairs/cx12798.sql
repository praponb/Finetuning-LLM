WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(is_new_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS is_new_session_avg_2,
        ROUND(AVG(is_new_session) OVER (PARTITION BY is_new_session, grade_status ORDER BY is_new_session DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS is_new_session_avg_3,
        ROUND(AVG(session_no) OVER (ORDER BY score_filled NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS session_no_avg_4,
        COUNT(*) OVER w_student_id AS rows_in_student_id,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id
    FROM sessionized
    WINDOW w_student_id AS (PARTITION BY student_id)
),
derived AS (
    -- Add calculated columns
    SELECT *, ROUND(session_no / NULLIF(score, 0), 3) AS session_no_per_score
    FROM with_windows
)
SELECT
    id,
    student_id,
    course_id,
    score,
    enrolled_at,
    prev_enrolled_at,
    is_new_session,
    session_no,
    is_new_session_avg_2,
    is_new_session_avg_3,
    session_no_avg_4,
    session_no_per_score
FROM derived
ORDER BY session_no_per_score NULLS LAST, id
LIMIT 10
