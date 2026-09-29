WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    course_id,
    burst_id,
    is_new_session,
    GROUPING(course_id) AS is_total_course_id,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list
FROM sessionized
GROUP BY CUBE(course_id, burst_id, is_new_session)
ORDER BY course_id NULLS LAST, burst_id NULLS LAST, is_new_session NULLS LAST
