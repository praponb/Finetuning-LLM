WITH scoped_students AS (
    -- Scope the students rows
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE major IN ('Math', 'Biz')
      AND UPPER(TRIM(student_name)) LIKE 'C%'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        gpa,
        ROUND(gpa - AVG(gpa) OVER w_major_cohort, 2) AS gpa_vs_major_cohort_avg,
        NTILE(5) OVER (ORDER BY gpa, id) AS gpa_quintile,
        gpa - LAG(gpa) OVER (PARTITION BY major ORDER BY enrolled_on, id) AS gpa_change,
        MAX(gpa) OVER (PARTITION BY major ORDER BY enrolled_on, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS gpa_max_6_prior,
        NTILE(3) OVER (ORDER BY gpa, id) AS gpa_tercile,
        FIRST_VALUE(gpa) IGNORE NULLS OVER (PARTITION BY major ORDER BY gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_gpa,
        DENSE_RANK() OVER (PARTITION BY major, is_international ORDER BY enrolled_on DESC, id) AS enrolled_on_dense_rank,
        ROUND(AVG(gpa) OVER (PARTITION BY major, cohort), 2) AS major_cohort_avg_gpa
    FROM scoped_students
    WINDOW w_major_cohort AS (PARTITION BY major, cohort)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        gpa,
        gpa_vs_major_cohort_avg,
        gpa_quintile,
        gpa_change,
        gpa_max_6_prior,
        gpa_tercile,
        first_known_gpa,
        major_cohort_avg_gpa
    FROM with_trends
    WHERE enrolled_on_dense_rank = 1
)
-- Final projection
SELECT
    id,
    cohort,
    major,
    gpa,
    gpa_vs_major_cohort_avg,
    gpa_max_6_prior,
    gpa_tercile,
    major_cohort_avg_gpa,
    COALESCE(gpa_max_6_prior, 0.0) AS gpa_max_6_prior_filled
FROM top_per_group
ORDER BY gpa DESC, id
LIMIT 10
