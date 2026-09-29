WITH filtered AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        COALESCE(major, 'UNKNOWN') AS major_or_unknown,
        CONCAT(cohort, '#', CAST(id AS STRING)) AS cohort_ref,
        CONCAT(cohort, '#', CAST(id AS STRING)) AS cohort_ref_2
    FROM students
    WHERE is_international = FALSE
      AND cohort = 'C2023'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        MONTH(enrolled_on) AS enrolled_on_month,
        COALESCE(major, 'UNKNOWN') AS major_or_unknown_2,
        ABS(gpa - 3.2) AS gpa_distance
    FROM filtered
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(enrolled_on) OVER (PARTITION BY major_or_unknown, enrolled_on_month ORDER BY gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_on,
        SUM(gpa_distance) OVER (PARTITION BY major, major_or_unknown) AS major_major_or_unknown_sum_gpa_distance,
        MIN(gpa) OVER w_major_major_or_unknown AS major_major_or_unknown_min_gpa
    FROM scored
    WINDOW w_major_major_or_unknown AS (PARTITION BY major, major_or_unknown)
)
SELECT
    id,
    student_name,
    major,
    enrolled_on,
    cohort_ref,
    cohort_ref_2,
    enrolled_on_month,
    gpa_distance,
    last_enrolled_on,
    major_major_or_unknown_sum_gpa_distance,
    major_major_or_unknown_min_gpa,
    CONCAT(major, '#', CAST(enrolled_on_month AS STRING)) AS major_ref,
    LOWER(TRIM(cohort_ref)) AS cohort_ref_clean
FROM metrics
