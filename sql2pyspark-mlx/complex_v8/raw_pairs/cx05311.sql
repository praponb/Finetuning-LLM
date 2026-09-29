WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE grade_status IS NOT NULL
      AND student_id <= 3
),
summary AS (
    -- Summarise per group
    SELECT
        course_id,
        DATE_FORMAT(enrolled_at_quarter, 'yyyy-MM') AS enrolled_at_quarter_ym,
        MIN(enrolled_at) AS first_enrolled_at,
        MIN(enrolled_at_quarter) AS first_enrolled_at_quarter,
        COUNT(*) FILTER (WHERE grade_status IS NOT NULL) AS n_grade_status_is_not_null,
        SUM(score) AS total_score,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS row_count
    FROM prepared
    GROUP BY course_id, DATE_FORMAT(enrolled_at_quarter, 'yyyy-MM')
    HAVING COUNT(*) >= 2
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM summary s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
        )
      AND s.course_id >= 11
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score >= 34.2
              AND e.grade_status <> 'PASS'
        )
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        course_id,
        enrolled_at_quarter_ym,
        first_enrolled_at,
        n_grade_status_is_not_null,
        MAX(total_score) OVER w_first_enrolled_at_year AS first_enrolled_at_year_max_total_score,
        DENSE_RANK() OVER w_row_count_n_grade_status_is AS total_score_dense_rank
    FROM screened
    WINDOW w_first_enrolled_at_year AS (PARTITION BY YEAR(first_enrolled_at)), w_row_count_n_grade_status_is AS (PARTITION BY row_count, n_grade_status_is_not_null ORDER BY total_score DESC, course_id, enrolled_at_quarter_ym)
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at
    FROM with_trends s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
SELECT
    course_id,
    n_grade_status_is_not_null,
    first_enrolled_at_year_max_total_score,
    total_score_dense_rank,
    last_enrollment_enrolled_at
FROM summarized_rows
ORDER BY first_enrolled_at_year_max_total_score NULLS LAST
