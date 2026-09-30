WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        YEAR(enrolled_at) AS enrolled_at_year
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND course_id IS NOT NULL
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status IS NOT NULL
              AND e.term <> '2024T2'
        )
      AND HOUR(s.enrolled_at_month) BETWEEN 10 AND 14
),
deduped AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        term_ref,
        enrolled_at_month,
        enrolled_at_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrolled_at_month DESC, id DESC) AS rn
        FROM screened
    ) ranked
    WHERE rn = 1
),
deduped_2 AS (
    -- Keep the latest row per student_id, grade_status
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        term_ref,
        enrolled_at_month,
        enrolled_at_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at DESC, id DESC) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM deduped_2
    WHERE enrolled_at_year > (SELECT AVG(enrolled_at_year) FROM deduped_2)
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        ROUND(AVG(score), 2) AS avg_enrollment_score,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.avg_enrollment_score,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM without_matches s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    grade_status,
    term_ref,
    enrolled_at_month,
    enrollment_count,
    LOWER(TRIM(term)) AS term_clean
FROM with_stats
WHERE grade_status = 'WITHDRAWN'
ORDER BY term_clean
