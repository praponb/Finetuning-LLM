WITH prepared AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        LOWER(REGEXP_REPLACE(TRIM(major), ' ', '_')) AS major_slug,
        SUBSTRING(TRIM(major), 1, 4) AS major_prefix
    FROM students
    WHERE NOT is_international
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY is_international, major) AS rows_in_is_international_major,
        NTH_VALUE(gpa, 2) OVER (PARTITION BY is_international, major ORDER BY gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa,
        LAST_VALUE(gpa) OVER (PARTITION BY major, cohort ORDER BY enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_gpa,
        COUNT(*) OVER (PARTITION BY cohort) AS rows_in_cohort,
        ROUND(gpa - AVG(gpa) OVER (PARTITION BY is_international, major), 2) AS gpa_vs_is_international_major_avg,
        MAX(gpa) OVER (PARTITION BY cohort) AS cohort_max_gpa,
        LEAD(enrolled_on, 2) OVER (PARTITION BY cohort ORDER BY gpa ASC, id) AS next2_enrolled_on,
        NTILE(5) OVER (PARTITION BY cohort ORDER BY gpa ASC, id) AS gpa_quintile,
        FIRST_VALUE(gpa) IGNORE NULLS OVER (PARTITION BY cohort ORDER BY gpa ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_gpa,
        SUM(gpa) OVER (PARTITION BY cohort) AS cohort_sum_gpa
    FROM prepared
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(cohort_max_gpa, 0.0) AS cohort_max_gpa_filled,
        ABS(cohort_sum_gpa - 20.8) AS cohort_sum_gpa_distance,
        COALESCE(major, 'UNKNOWN') AS major_or_unknown
    FROM with_windows
)
SELECT
    id,
    cohort,
    major,
    gpa,
    is_international,
    major_slug,
    rows_in_is_international_major,
    next2_enrolled_on,
    gpa_quintile,
    first_known_gpa,
    cohort_sum_gpa,
    cohort_sum_gpa_distance,
    major_or_unknown,
    CONCAT_WS(' / ', major_or_unknown, student_name) AS major_or_unknown_student_name_label,
    YEAR(enrolled_on) AS enrolled_on_year
FROM derived
