WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        is_international,
        ABS(gpa - 3.2) AS gpa_distance,
        CONCAT(student_name, '#', CAST(id AS STRING)) AS student_name_ref
    FROM students
    WHERE cohort IN ('C2022', 'C2024')
),
unique_rows AS (
    -- Keep the earliest row per cohort
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        is_international,
        gpa_distance,
        student_name_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY enrolled_on, id) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_on, cohort, gpa_distance
    FROM unique_rows
) src
PIVOT (
    COUNT(gpa_distance) AS count_v, SUM(gpa_distance) AS sum_v
    FOR cohort IN ('C2023', 'C2024')
)
ORDER BY enrolled_on
