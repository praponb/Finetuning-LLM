WITH scoped_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        gpa,
        is_international,
        COALESCE(gpa, 0.0) AS gpa_filled,
        COALESCE(cohort, 'UNKNOWN') AS cohort_or_unknown,
        YEAR(enrolled_on) AS enrolled_on_year
    FROM students
    WHERE major IN ('Math', 'Bio')
      AND gpa < 3.3
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(cohort)) AS cohort_len,
        DATE_TRUNC('QUARTER', enrolled_on) AS enrolled_on_quarter,
        LEAST(gpa, enrolled_on_year) AS least_gpa_enrolled_on_year
    FROM scoped_students
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY cohort_or_unknown, is_international) AS rows_in_cohort_or_unknown_is_international,
        MAX(gpa_filled) OVER (PARTITION BY cohort_len ORDER BY enrolled_on_quarter ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_gpa_filled_so_far,
        NTH_VALUE(gpa_filled, 2) OVER (PARTITION BY cohort_len ORDER BY enrolled_on_quarter ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa_filled,
        ROUND(AVG(gpa_filled) OVER (PARTITION BY cohort_or_unknown, is_international), 2) AS cohort_or_unknown_is_international_avg_gpa_fille,
        LAST_VALUE(gpa) IGNORE NULLS OVER (PARTITION BY cohort_len ORDER BY enrolled_on_quarter ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_gpa
    FROM derived
)
SELECT
    id,
    student_name,
    cohort,
    enrolled_on,
    gpa,
    is_international,
    gpa_filled,
    cohort_or_unknown,
    enrolled_on_year,
    enrolled_on_quarter,
    rows_in_cohort_or_unknown_is_international,
    max_gpa_filled_so_far,
    second_gpa_filled,
    cohort_or_unknown_is_international_avg_gpa_fille,
    HOUR(enrolled_on_quarter) AS enrolled_on_quarter_hour
FROM with_trends
ORDER BY cohort, rows_in_cohort_or_unknown_is_international
