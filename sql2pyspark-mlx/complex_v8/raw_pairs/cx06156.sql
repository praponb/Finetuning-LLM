WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        SUBSTRING(TRIM(term), 1, 4) AS term_prefix,
        CONCAT_WS(': ', term, grade_status) AS term_grade_status_label,
        CONCAT_WS(' / ', term, grade_status) AS term_grade_status_label_2
    FROM enrollments
    WHERE (course_id <= 7 OR grade_status NOT IN ('WITHDRAWN', 'INCOMPLETE'))
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(term), 1, 4) AS term_prefix_2,
        FLOOR(score / 25) * 25 AS score_bucket,
        CASE WHEN score >= 77.0 THEN 'A' WHEN score >= 39.4 THEN 'B' ELSE 'C' END AS score_tier
    FROM filtered_enrollments
),
with_details_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.term_prefix,
        s.term_grade_status_label,
        s.term_grade_status_label_2,
        s.term_prefix_2,
        st.cohort AS student_cohort
    FROM enriched s
    LEFT JOIN students st ON st.id = s.student_id
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_details_student s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND HOUR(e.enrolled_at) BETWEEN 10 AND 16
        )
      AND s.student_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    student_id,
    grade_status,
    term_prefix,
    term_grade_status_label,
    term_grade_status_label_2,
    term_prefix_2,
    student_cohort,
    LOWER(TRIM(grade_status)) AS grade_status_clean
FROM without_matches
WHERE term_prefix IS NOT NULL
ORDER BY term_prefix_2 DESC, id
LIMIT 5
