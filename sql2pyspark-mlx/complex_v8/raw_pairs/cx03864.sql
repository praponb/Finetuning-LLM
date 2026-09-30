WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 2)
      AND student_id <= 10
),
combined_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_date,
        s.term_ref,
        c.dept AS course_dept,
        c.course_code
    FROM cleaned s
    JOIN courses c ON c.id = s.course_id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.enrolled_at_date,
        s.term_ref,
        s.course_code,
        c.credits AS course_credits
    FROM combined_details_course s
    JOIN courses c ON c.id = s.course_id
    WHERE c.credits < 3
)
SELECT DISTINCT student_id
FROM joined
WHERE student_id IS NOT NULL
ORDER BY student_id DESC
