WITH cleaned AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        is_international,
        DATE_FORMAT(enrolled_on, 'yyyy-MM-dd') AS enrolled_on_label
    FROM students
    WHERE enrolled_on >= DATE '2024-05-18'
      AND cohort <> 'C2021'
),
tagged AS (
    -- Add calculated columns
    SELECT *, COALESCE(cohort, 'UNKNOWN') AS cohort_or_unknown
    FROM cleaned
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.grade_status = 'INCOMPLETE'
              AND e.enrolled_at < TIMESTAMP '2025-02-18 08:00:00'
        )
),
per_group AS (
    -- Group-level totals
    SELECT
        is_international,
        cohort_or_unknown,
        cohort,
        COUNT(*) FILTER (WHERE is_international = FALSE) AS n_is_international_false,
        COUNT(*) FILTER (WHERE UPPER(TRIM(student_name)) LIKE 'C%') AS n_upper_trim_student_name_like,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT cohort_or_unknown) AS distinct_cohort_or_unknowns,
        ARRAY_SORT(COLLECT_SET(student_name)) AS student_name_list
    FROM qualified
    WHERE cohort_or_unknown = 'C2022'
    GROUP BY is_international, cohort_or_unknown, cohort
)
SELECT *
FROM (
    SELECT distinct_cohort_or_unknowns, cohort, cohort_or_unknown, cnt
    FROM per_group
) src
PIVOT (
    MAX(cnt) AS max_v, COUNT(cnt) AS n
    FOR cohort_or_unknown IN ('C2024', 'UNKNOWN')
)
