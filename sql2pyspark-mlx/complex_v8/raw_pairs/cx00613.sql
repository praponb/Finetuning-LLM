WITH recent AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        is_international,
        DATE_TRUNC('YEAR', enrolled_on) AS enrolled_on_year,
        COALESCE(gpa, 0.0) AS gpa_filled
    FROM students
    WHERE UPPER(TRIM(student_name)) LIKE 'B%'
      AND is_international = FALSE
),
latest AS (
    -- Keep the earliest row per major, cohort
    SELECT
        id,
        student_name,
        cohort,
        major,
        is_international,
        enrolled_on_year,
        gpa_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY major, cohort ORDER BY gpa_filled, id) AS recency_rank
        FROM recent
    ) ranked
    WHERE recency_rank = 1
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DAYOFWEEK(enrolled_on_year) AS enrolled_on_year_dayofweek,
        LOWER(REGEXP_REPLACE(TRIM(student_name), ' ', '_')) AS student_name_slug
    FROM latest
),
deduped AS (
    -- Keep the earliest row per major, cohort
    SELECT
        id,
        student_name,
        cohort,
        major,
        is_international,
        enrolled_on_year,
        gpa_filled,
        enrolled_on_year_dayofweek,
        student_name_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY major, cohort ORDER BY gpa_filled, id) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
latest_per_key AS (
    -- Keep the latest row per major, cohort
    SELECT
        id,
        student_name,
        cohort,
        major,
        is_international,
        enrolled_on_year,
        gpa_filled,
        enrolled_on_year_dayofweek,
        student_name_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY major, cohort ORDER BY gpa_filled DESC, id DESC) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        cohort,
        is_international,
        enrolled_on_year,
        enrolled_on_year_dayofweek,
        student_name_slug,
        DENSE_RANK() OVER (ORDER BY enrolled_on_year_dayofweek ASC, id) AS enrolled_on_year_dayofweek_dense_rank,
        COUNT(*) OVER (PARTITION BY major) AS rows_in_major,
        SIZE(COLLECT_SET(cohort) OVER w_major) AS distinct_cohort_in_major,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_on_year_dayofweek ASC, id), 3) AS enrolled_on_year_dayofweek_percent_rank,
        NTILE(4) OVER (ORDER BY enrolled_on_year_dayofweek ASC, id) AS enrolled_on_year_dayofweek_quartile
    FROM latest_per_key
    WINDOW w_major AS (PARTITION BY major)
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.enrolled_on_year_dayofweek,
        s.rows_in_major,
        s.distinct_cohort_in_major,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM with_trends s
    JOIN enrollments e ON e.student_id = s.id
)
SELECT
    id,
    enrolled_on_year_dayofweek,
    rows_in_major,
    distinct_cohort_in_major,
    enrollment_id,
    enrollment_score,
    enrollment_course_id,
    enrollment_enrolled_at
FROM exploded_detail_enrollments
ORDER BY enrollment_enrolled_at NULLS LAST
