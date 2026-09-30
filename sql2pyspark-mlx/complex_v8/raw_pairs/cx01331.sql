WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE grade_status = 'FAIL'
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM cleaned_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM sessionized s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        term,
        grade_status,
        enrolled_at,
        grade_status_term_label,
        is_new_session,
        session_id,
        NTILE(5) OVER (PARTITION BY grade_status, session_id ORDER BY enrolled_at, id) AS enrolled_at_quintile,
        NTH_VALUE(score, 2) OVER (PARTITION BY session_id, is_new_session ORDER BY session_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        NTILE(3) OVER w_session_id_is_new_session_by_2 AS session_id_tercile,
        RANK() OVER (PARTITION BY grade_status, session_id ORDER BY enrolled_at) AS enrolled_at_rank,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY student_id ORDER BY session_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        RANK() OVER w_grade_status_session_id_by_e AS enrolled_at_rank_2
    FROM screened
    WINDOW w_session_id_is_new_session_by_2 AS (PARTITION BY session_id, is_new_session ORDER BY session_id, id), w_grade_status_session_id_by_e AS (PARTITION BY grade_status, session_id ORDER BY enrolled_at, id)
)
SELECT
    id,
    term,
    grade_status,
    grade_status_term_label,
    session_id,
    enrolled_at_quintile,
    second_score,
    session_id_tercile,
    enrolled_at_rank,
    second_enrolled_at,
    enrolled_at_rank_2,
    FLOOR(second_score / 25) * 25 AS second_score_bucket,
    CONCAT_WS('|', term, grade_status_term_label) AS term_grade_status_term_label_label
FROM with_trends
ORDER BY term_grade_status_term_label_label, session_id DESC
