WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        COALESCE(gpa, 0.0) AS gpa_filled,
        CASE WHEN cohort IN ('C2021', 'C2023') THEN 'group_a' WHEN cohort = 'C2022' THEN 'group_b' ELSE 'other' END AS cohort_group,
        SUBSTRING(TRIM(cohort), 1, 4) AS cohort_prefix
    FROM students
    WHERE (LENGTH(TRIM(student_name)) >= 5 OR gpa > 2.7)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_cohort_group AS rows_in_cohort_group,
        RANK() OVER (PARTITION BY cohort_group, major ORDER BY gpa_filled) AS gpa_filled_rank,
        DENSE_RANK() OVER (PARTITION BY cohort_group, major ORDER BY gpa_filled, id) AS gpa_filled_dense_rank
    FROM prepared
    WINDOW w_cohort_group AS (PARTITION BY cohort_group)
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.term <> '2025T1'
        )
)
-- Final projection
SELECT
    id,
    cohort,
    major,
    enrolled_on,
    gpa_filled,
    cohort_group,
    cohort_prefix,
    rows_in_cohort_group,
    gpa_filled_rank,
    gpa_filled_dense_rank,
    ROUND(gpa_filled * 1.5, 2) AS gpa_filled_adj,
    DATE_FORMAT(enrolled_on, 'MMM yyyy') AS enrolled_on_label
FROM qualified
WHERE major IN ('Art', 'CS')
