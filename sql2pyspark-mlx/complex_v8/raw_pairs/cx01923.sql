WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE score <= 73.0
      AND grade_status = 'PASS'
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at
    FROM prepared_enrollments s
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(term, 'ALL') AS term,
    COALESCE(grade_status, 'ALL') AS grade_status,
    GROUPING(term) AS is_total_term,
    COUNT(DISTINCT term) AS distinct_terms,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(STDDEV(score), 2) AS stddev_score
FROM summarized_rows
GROUP BY ROLLUP(term, grade_status)
