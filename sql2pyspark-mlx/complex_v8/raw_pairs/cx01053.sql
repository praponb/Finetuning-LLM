WITH base AS (
    -- Filter enrollments
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
    WHERE student_id IS NOT NULL
      AND grade_status IN ('INCOMPLETE', 'WITHDRAWN', 'PASS')
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        score_filled,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label_2
    FROM base
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score <= 57.6
              AND e.term = '2025T1'
        )
),
enriched AS (
    -- Add calculated columns
    SELECT *, ABS(days_since_enrolled_at - 315) AS days_since_enrolled_at_distance
    FROM without_matches
)
SELECT
    course_id,
    student_id,
    GROUPING(course_id) AS is_total_course_id,
    ROUND(AVG(days_since_enrolled_at_distance), 2) AS avg_days_since_enrolled_at_distance,
    COUNT(*) AS row_count
FROM enriched
GROUP BY CUBE(course_id, student_id)
ORDER BY course_id NULLS LAST, student_id NULLS LAST
