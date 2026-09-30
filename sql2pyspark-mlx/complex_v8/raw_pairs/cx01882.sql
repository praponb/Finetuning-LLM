WITH prepared_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        INITCAP(TRIM(cohort)) AS cohort_clean,
        ADD_MONTHS(enrolled_on, 1) AS enrolled_on_plus_1m
    FROM students
    WHERE student_name IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(gpa / NULLIF(SUM(gpa) OVER (PARTITION BY YEAR(enrolled_on_plus_1m), major), 0), 4) AS gpa_share_of_enrolled_on_plus_1m_year_major,
        SUM(gpa) OVER (PARTITION BY YEAR(enrolled_on_plus_1m), major) AS enrolled_on_plus_1m_year_major_sum_gpa,
        RANK() OVER w_major_cohort_by_enrolled_on AS enrolled_on_rank,
        SUM(gpa) OVER (ORDER BY enrolled_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS gpa_sum_4,
        NTH_VALUE(enrolled_on_plus_1m, 2) OVER (PARTITION BY major, cohort ORDER BY enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_on_plus_1m
    FROM prepared_students
    WINDOW w_major_cohort_by_enrolled_on AS (PARTITION BY major, cohort ORDER BY enrolled_on DESC)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        enrolled_on_plus_1m,
        enrolled_on_plus_1m_year_major_sum_gpa,
        enrolled_on_rank,
        gpa_sum_4,
        second_enrolled_on_plus_1m,
        DATEDIFF(DATE '2025-12-31', enrolled_on_plus_1m) AS days_since_enrolled_on_plus_1m,
        LAST_DAY(enrolled_on) AS enrolled_on_month_end
    FROM ranked
    WHERE gpa_sum_4 > 5.2
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.id) AS max_enrollment_score,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS first_enrollment_enrolled_at,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS first_enrollment_enrolled_at_2
    FROM labeled s
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS first_enrollment_enrolled_at_3
    FROM with_counts s
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        UPPER(TRIM(major)) AS major_clean,
        INITCAP(TRIM(major)) AS major_clean_2,
        CONCAT(student_name, '#', CAST(enrolled_on_rank AS STRING)) AS student_name_ref
    FROM with_stats
    WHERE enrolled_on_rank > 4
)
SELECT *
FROM (
    SELECT first_enrollment_enrolled_at_3, major, enrolled_on_plus_1m_year_major_sum_gpa
    FROM enriched
) src
PIVOT (
    SUM(enrolled_on_plus_1m_year_major_sum_gpa)
    FOR major IN ('Art', 'Biz', 'Bio', 'Math')
)
ORDER BY first_enrollment_enrolled_at_3
