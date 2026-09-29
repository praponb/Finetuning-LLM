WITH scoped_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        SUBSTRING(TRIM(student_name), 1, 4) AS student_name_prefix,
        ROUND(gpa * 1.5, 2) AS gpa_adj
    FROM students
    WHERE LOWER(student_name) LIKE '%sc%'
      AND is_international = FALSE
),
cohort_stats AS (
    -- Avg gpa_adj per cohort
    SELECT cohort, AVG(gpa_adj) AS avg_gpa_adj
    FROM scoped_students
    GROUP BY cohort
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_gpa_adj,
        ROUND(s.gpa_adj - g.avg_gpa_adj, 2) AS gpa_adj_minus_avg
    FROM scoped_students s
    JOIN cohort_stats g ON g.cohort = s.cohort
    WHERE s.gpa_adj > g.avg_gpa_adj
)
SELECT *
FROM (
    SELECT is_international, enrolled_on, cohort, gpa
    FROM vs_group
) src
PIVOT (
    MIN(gpa)
    FOR cohort IN ('C2022', 'C2023')
)
ORDER BY is_international, enrolled_on
