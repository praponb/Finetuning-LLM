WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE score < 87.8
),
summary AS (
    -- Group-level totals
    SELECT
        term,
        student_id,
        course_id,
        MIN(enrolled_at) AS first_enrolled_at,
        MAX(enrolled_at) AS last_enrolled_at
    FROM prepared_enrollments
    GROUP BY term, student_id, course_id
)
SELECT
    course_id,
    COALESCE(term, 'ALL') AS term,
    COUNT(*) AS row_count,
    SUM(CASE WHEN term <> '2024T1' THEN 1 ELSE 0 END) AS n_term_2024t1,
    MAX(first_enrolled_at) AS last_first_enrolled_at
FROM summary
GROUP BY CUBE(course_id, term)
ORDER BY course_id NULLS LAST, term NULLS LAST
