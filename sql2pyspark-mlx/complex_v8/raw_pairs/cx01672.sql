WITH filtered_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        ADD_MONTHS(enrolled_on, 6) AS enrolled_on_plus_6m,
        SUBSTRING(TRIM(major), 1, 2) AS major_prefix,
        YEAR(enrolled_on) AS enrolled_on_year
    FROM students
    WHERE NOT is_international
      AND cohort <> 'C2022'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_enrolled_on_plus_6m_month_en AS enrolled_on_plus_6m_dense_rank,
        ROUND(enrolled_on_year / NULLIF(SUM(enrolled_on_year) OVER w_cohort, 0), 4) AS enrolled_on_year_share_of_cohort,
        ROUND(gpa / NULLIF(SUM(gpa) OVER (PARTITION BY cohort), 0), 4) AS gpa_share_of_cohort,
        LAST_VALUE(enrolled_on_year) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_on_plus_6m), enrolled_on_year ORDER BY enrolled_on_plus_6m, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_on_year,
        ROUND(AVG(gpa) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_on_plus_6m), enrolled_on_year ORDER BY enrolled_on_plus_6m, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS gpa_avg_3
    FROM filtered_students
    WINDOW w_enrolled_on_plus_6m_month_en AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_on_plus_6m), enrolled_on_year ORDER BY enrolled_on_plus_6m), w_cohort AS (PARTITION BY cohort)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(gpa_share_of_cohort / NULLIF(gpa_avg_3, 0), 3) AS gpa_share_of_cohort_per_gpa_avg_3,
        DATEDIFF(enrolled_on_plus_6m, enrolled_on) AS days_enrolled_on_to_enrolled_on_plus_6m
    FROM windowed
    WHERE cohort <> 'C2023'
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(score) AS min_enrollment_score,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM annotated s
    LEFT JOIN enrollment_stats e ON e.student_id = s.id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_name,
        gpa,
        is_international,
        major_prefix,
        enrolled_on_plus_6m_dense_rank,
        enrolled_on_year_share_of_cohort,
        gpa_share_of_cohort,
        last_enrolled_on_year,
        gpa_avg_3,
        gpa_share_of_cohort_per_gpa_avg_3,
        days_enrolled_on_to_enrolled_on_plus_6m,
        min_enrollment_score,
        enrollment_count,
        SUM(enrolled_on_year) OVER w_enrolled_on_plus_6m_dense_ra AS enrolled_on_plus_6m_dense_rank_enrolled_on_year_,
        MIN(gpa) OVER (PARTITION BY enrolled_on_plus_6m_dense_rank, enrolled_on_year) AS enrolled_on_plus_6m_dense_rank_enrolled_on_year__2
    FROM profiled
    WINDOW w_enrolled_on_plus_6m_dense_ra AS (PARTITION BY enrolled_on_plus_6m_dense_rank, enrolled_on_year)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(gpa_avg_3 / NULLIF(enrollment_count, 0), 3) AS gpa_avg_3_per_enrollment_count
    FROM ranked
    WHERE major_prefix IS NOT NULL
)
SELECT
    id,
    gpa,
    is_international,
    major_prefix,
    enrolled_on_plus_6m_dense_rank,
    enrolled_on_year_share_of_cohort,
    last_enrolled_on_year,
    gpa_avg_3,
    gpa_share_of_cohort_per_gpa_avg_3,
    days_enrolled_on_to_enrolled_on_plus_6m,
    min_enrollment_score,
    enrollment_count,
    enrolled_on_plus_6m_dense_rank_enrolled_on_year_,
    enrolled_on_plus_6m_dense_rank_enrolled_on_year__2,
    gpa_avg_3_per_enrollment_count
FROM scored
WHERE NOT is_international
ORDER BY enrollment_count, min_enrollment_score, id
LIMIT 10
