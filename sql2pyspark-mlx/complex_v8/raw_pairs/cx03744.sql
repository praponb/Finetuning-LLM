WITH prepared_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        DATEDIFF(DATE '2025-06-30', enrolled_on) AS days_since_enrolled_on,
        CONCAT_WS(': ', major, cohort) AS major_cohort_label
    FROM students
    WHERE gpa <= 3.4
      AND is_international = TRUE
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        days_since_enrolled_on,
        major_cohort_label,
        ROUND(PERCENT_RANK() OVER (PARTITION BY major ORDER BY enrolled_on DESC, id), 3) AS enrolled_on_percent_rank,
        MAX(gpa) OVER (PARTITION BY cohort ORDER BY days_since_enrolled_on DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS gpa_max_7,
        ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY days_since_enrolled_on DESC, id) AS seq_no,
        ROUND(days_since_enrolled_on / NULLIF(SUM(days_since_enrolled_on) OVER (PARTITION BY major), 0), 4) AS days_since_enrolled_on_share_of_major,
        ROUND(AVG(gpa) OVER (PARTITION BY major ORDER BY enrolled_on DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS gpa_avg_2_prior
    FROM prepared_students
),
labeled AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(enrolled_on, 'MMM yyyy') AS enrolled_on_label
    FROM windowed
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        days_since_enrolled_on,
        major_cohort_label,
        enrolled_on_percent_rank,
        gpa_max_7,
        days_since_enrolled_on_share_of_major,
        gpa_avg_2_prior,
        enrolled_on_label
    FROM labeled
    WHERE seq_no <= 3
      AND days_since_enrolled_on_share_of_major < 0.8
)
SELECT
    major,
    cohort,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT major) AS distinct_majors
FROM top_per_group
GROUP BY GROUPING SETS ((major, cohort), (major), (cohort), ())
ORDER BY major NULLS LAST, cohort NULLS LAST
