WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        LOWER(REGEXP_REPLACE(TRIM(student_name), ' ', '_')) AS student_name_slug,
        CONCAT_WS('-', student_name, major) AS student_name_major_label,
        TRUNC(enrolled_on, 'MM') AS enrolled_on_month_start
    FROM students
    WHERE cohort = 'C2023'
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.cohort,
        s.enrolled_on,
        s.gpa,
        s.is_international,
        s.student_name_slug,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at
    FROM valid s
    LEFT JOIN enrollments e ON e.student_id = s.id
)
SELECT
    enrollment_term,
    enrollment_id,
    COUNT(*) AS row_count,
    ROUND(SUM(enrollment_score) / NULLIF(SUM(gpa), 0), 3) AS enrollment_score_to_gpa_ratio,
    COUNT(*) FILTER (WHERE cohort <> 'C2022') AS n_cohort_c2022
FROM detail_rows
GROUP BY ROLLUP(enrollment_term, enrollment_id)
ORDER BY enrollment_term NULLS LAST, enrollment_id NULLS LAST
