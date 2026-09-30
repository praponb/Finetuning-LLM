WITH valid AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        COALESCE(major, 'UNKNOWN') AS major_or_unknown
    FROM students
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN e.grade_status = 'FAIL' THEN 1 END) FROM enrollments e WHERE e.student_id = s.id) AS enrollment_grade_status_count,
        (SELECT COUNT(*) FILTER (WHERE e.term = '2024T1') FROM enrollments e WHERE e.student_id = s.id) AS enrollment_term_count
    FROM valid s
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.is_international,
        s.major_or_unknown,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score,
        e.course_id AS enrollment_course_id,
        e.term AS enrollment_term
    FROM profiled s
    LEFT JOIN enrollments e ON e.student_id = s.id
)
SELECT *
FROM (
    SELECT enrollment_grade_status, major_or_unknown, enrollment_score
    FROM with_children
) src
PIVOT (
    SUM(enrollment_score)
    FOR major_or_unknown IN ('Bio', 'Math', 'CS', 'Art')
)
