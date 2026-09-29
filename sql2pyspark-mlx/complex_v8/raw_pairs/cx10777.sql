WITH scoped_enrollments AS (
    -- Filter enrollments
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
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM scoped_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        enrolled_at,
        burst_id,
        ROW_NUMBER() OVER w_new_session_flag_by_score AS score_seq,
        ROUND(new_session_flag / NULLIF(SUM(new_session_flag) OVER w_grade_status, 0), 4) AS new_session_flag_share_of_grade_status
    FROM sessionized
    WINDOW w_new_session_flag_by_score AS (PARTITION BY new_session_flag ORDER BY score, id), w_grade_status AS (PARTITION BY grade_status)
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    score,
    grade_status,
    enrolled_at,
    score_seq,
    new_session_flag_share_of_grade_status
FROM sequenced
WHERE enrolled_at < TIMESTAMP '2025-03-02 08:00:00'
ORDER BY id DESC, score_seq
