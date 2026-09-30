WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        HOUR(enrolled_at) AS enrolled_at_hour,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE (grade_status IS NOT NULL OR score > 5.9)
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score,
        (SELECT COUNT(*) FILTER (WHERE e.grade_status IN ('PASS', 'INCOMPLETE', 'WITHDRAWN')) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score
    FROM prepared_enrollments s
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        YEAR(enrolled_at) AS enrolled_at_year,
        GREATEST(avg_enrollment_score, enrolled_at_quarter) AS greatest_avg_enrollment_score_enrolled_at_quarte,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM profiled
)
-- Final projection
SELECT
    id,
    term,
    score,
    enrolled_at,
    enrolled_at_hour,
    enrolled_at_quarter,
    avg_enrollment_score,
    enrollment_grade_status_count,
    min_enrollment_score,
    enrolled_at_year,
    greatest_avg_enrollment_score_enrolled_at_quarte
FROM labeled
WHERE TO_DATE(enrolled_at) < DATE '2025-03-14'
ORDER BY score, enrollment_grade_status_count DESC NULLS FIRST, id
LIMIT 5
