WITH valid_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        gpa,
        is_international,
        DATE_FORMAT(enrolled_on, 'MMM yyyy') AS enrolled_on_label
    FROM students
    WHERE gpa < 3.4
),
stacked AS (
    -- Stack the branches
    SELECT
        'student' AS origin,
        student_name AS category,
        cohort,
        gpa
    FROM valid_students
    WHERE cohort IN ('C2023', 'C2021', 'C2022')
    UNION ALL
    SELECT
        'enrollment' AS origin,
        grade_status AS category,
        term AS cohort,
        score AS gpa
    FROM enrollments
    UNION ALL
    SELECT
        'enrollment' AS origin,
        term AS category,
        grade_status AS cohort,
        score AS gpa
    FROM enrollments
)
SELECT origin, COUNT(*) AS n_rows, SUM(gpa) AS total_gpa
FROM stacked
GROUP BY origin
ORDER BY origin
