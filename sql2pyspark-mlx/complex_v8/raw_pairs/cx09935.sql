WITH cleaned_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        major,
        gpa,
        is_international,
        COALESCE(cohort, 'UNKNOWN') AS cohort_or_unknown,
        QUARTER(enrolled_on) AS enrolled_on_quarter,
        UPPER(TRIM(student_name)) AS student_name_clean
    FROM students
    WHERE NOT is_international
      AND major = 'Biz'
),
latest_per_key AS (
    -- Keep the latest row per cohort_or_unknown, major
    SELECT
        id,
        student_name,
        major,
        gpa,
        is_international,
        cohort_or_unknown,
        enrolled_on_quarter,
        student_name_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cohort_or_unknown, major ORDER BY enrolled_on_quarter DESC, id DESC) AS row_num
        FROM cleaned_students
    ) ranked
    WHERE row_num = 1
)
SELECT
    cohort_or_unknown,
    is_international,
    enrolled_on_quarter,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(student_name)) AS student_name_list
FROM latest_per_key
GROUP BY ROLLUP(cohort_or_unknown, is_international, enrolled_on_quarter)
ORDER BY cohort_or_unknown NULLS LAST, is_international NULLS LAST, enrolled_on_quarter NULLS LAST
