WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE is_international = TRUE
      AND major <> 'Math'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY major, is_international ORDER BY gpa ASC, id) AS gpa_dense_rank,
        COUNT(*) OVER (PARTITION BY major) AS rows_in_major,
        RANK() OVER (PARTITION BY major, is_international ORDER BY gpa ASC) AS gpa_rank,
        DATEDIFF(enrolled_on, LAG(enrolled_on) OVER (PARTITION BY cohort, YEAR(enrolled_on) ORDER BY gpa DESC, id)) AS days_since_prev_enrolled_on,
        MAX(gpa) OVER (PARTITION BY cohort, YEAR(enrolled_on) ORDER BY gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_gpa_so_far
    FROM base
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM ranked
    WHERE gpa < (SELECT MAX(gpa) FROM ranked)
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        gpa_dense_rank,
        rows_in_major,
        gpa_rank,
        days_since_prev_enrolled_on,
        max_gpa_so_far,
        ROUND(max_gpa_so_far - AVG(max_gpa_so_far) OVER (PARTITION BY cohort), 2) AS max_gpa_so_far_vs_cohort_avg,
        LEAD(enrolled_on) OVER (PARTITION BY is_international ORDER BY gpa_dense_rank, id) AS next_enrolled_on,
        ROUND(AVG(gpa_dense_rank) OVER (PARTITION BY is_international ORDER BY max_gpa_so_far, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS gpa_dense_rank_avg_3
    FROM with_activity
),
with_activity_2 AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM ranked_2 s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.course_id IS NOT NULL
        )
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        e.total_enrollment_score
    FROM with_activity_2 s
    LEFT JOIN enrollment_stats e ON e.student_id = s.id
)
-- Final projection
SELECT
    id,
    cohort,
    major,
    is_international,
    rows_in_major,
    gpa_rank,
    max_gpa_so_far,
    max_gpa_so_far_vs_cohort_avg,
    next_enrolled_on,
    gpa_dense_rank_avg_3,
    last_enrollment_enrolled_at,
    ADD_MONTHS(enrolled_on, 12) AS enrolled_on_plus_12m
FROM profiled
WHERE gpa_dense_rank_avg_3 >= 2.4
ORDER BY id NULLS LAST
