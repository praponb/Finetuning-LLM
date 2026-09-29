WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE course_id <= 6
      AND score >= 10.8
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM recent s
),
latest_per_key AS (
    -- Keep the latest row per course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_grade_status_label,
        enrolled_at_date,
        first_enrollment_enrolled_at,
        enrollment_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY enrolled_at_date DESC, id DESC) AS row_num
        FROM with_counts
    ) ranked
    WHERE row_num = 1
)
SELECT *
FROM (
    SELECT enrollment_count, enrolled_at, grade_status, score
    FROM latest_per_key
) src
PIVOT (
    SUM(score)
    FOR grade_status IN ('WITHDRAWN', 'PASS', 'INCOMPLETE')
)
ORDER BY enrollment_count, enrolled_at
