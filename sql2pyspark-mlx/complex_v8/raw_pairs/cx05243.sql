WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE grade_status IN ('PASS', 'INCOMPLETE', 'WITHDRAWN')
      AND score >= 38.3
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM prepared_enrollments
    WHERE score <= 79.3
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at
    FROM derived s
)
SELECT
    term_or_unknown,
    grade_status,
    course_id,
    SUM(CASE WHEN term <> '2025T2' THEN score ELSE 0 END) AS score_term_2025t2
FROM profiled
GROUP BY CUBE(term_or_unknown, grade_status, course_id)
