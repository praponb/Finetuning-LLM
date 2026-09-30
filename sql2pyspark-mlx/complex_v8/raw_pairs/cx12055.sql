WITH valid AS (
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
    WHERE student_id >= 3
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) FILTER (WHERE grade_status = 'FAIL') AS enrollment_grade_status_count
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count
    FROM valid s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.course_id AS enrollment_course_id,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status
    FROM with_stats s
    JOIN enrollments e ON e.student_id = s.student_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER w_enrollment_course_id_by_enro AS enrolled_at_quintile,
        MAX(enrollment_score) OVER (PARTITION BY enrollment_course_id ORDER BY enrolled_at, id, enrollment_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS enrollment_score_max_1_prior,
        LEAD(enrolled_at) OVER w_enrollment_course_id_by_enro AS next_enrolled_at,
        COUNT(*) OVER (PARTITION BY enrollment_id, enrollment_grade_status) AS rows_in_enrollment_id_enrollment_grade_status,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY enrollment_id, enrollment_grade_status)) AS distinct_grade_status_in_enrollment_id_enrollmen
    FROM with_children_enrollments
    WINDOW w_enrollment_course_id_by_enro AS (PARTITION BY enrollment_course_id ORDER BY enrolled_at, id, enrollment_id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrollment_id, enrollment_grade_status, rows_in_enrollment_id_enrollment_grade_status
    FROM with_windows
) src
PIVOT (
    AVG(rows_in_enrollment_id_enrollment_grade_status)
    FOR enrollment_grade_status IN ('INCOMPLETE', 'WITHDRAWN', 'FAIL', 'PASS')
)
