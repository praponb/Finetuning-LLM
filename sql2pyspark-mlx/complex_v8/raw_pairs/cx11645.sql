WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE score <= 58.8
      AND course_id IS NOT NULL
),
enriched_course AS (
    -- Attach course attributes
    SELECT s.*, c.dept AS course_dept, c.course_code
    FROM filtered_enrollments s
    JOIN courses c ON c.id = s.course_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        course_dept,
        course_code,
        DATE_TRUNC('HOUR', enrolled_at) AS enrolled_at_hour,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM enriched_course
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        DATE_FORMAT(enrolled_at_hour, 'yyyy-MM-dd') AS enrolled_at_hour_label
    FROM enriched
    WHERE score_filled >= 35.0
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT grade_status, course_dept, score_filled
    FROM enriched_2
) src
PIVOT (
    SUM(score_filled) AS amount, MAX(score_filled) AS max_v
    FOR course_dept IN ('CS', 'Art', 'Bio', 'Math')
)
ORDER BY grade_status
