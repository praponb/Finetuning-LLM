WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown_2
    FROM enrollments
    WHERE term = '2024T2'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        term_or_unknown_2,
        ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id) AS rn,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at
    FROM base
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        course_id,
        grade_status,
        enrolled_at,
        term_or_unknown_2,
        rn,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        MONTH(first_enrolled_at) AS first_enrolled_at_month,
        MONTH(enrolled_at) AS enrolled_at_month
    FROM sequenced
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        grade_status,
        term_or_unknown_2,
        rn,
        first_enrolled_at_month,
        enrolled_at_month,
        ROUND(AVG(first_enrolled_at_month) OVER w_grade_status, 2) AS grade_status_avg_first_enrolled_at_month,
        NTH_VALUE(enrolled_at_month, 2) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_month,
        DENSE_RANK() OVER w_all_by_enrolled_at_month AS enrolled_at_month_dense_rank
    FROM derived
    WINDOW w_grade_status AS (PARTITION BY grade_status), w_all_by_enrolled_at_month AS (ORDER BY enrolled_at_month DESC, id)
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        MIN(score) AS min_enrollment_score,
        ROUND(AVG(score), 2) AS avg_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.min_enrollment_score,
        e.avg_enrollment_score
    FROM with_trends s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        course_id,
        grade_status,
        term_or_unknown_2,
        first_enrolled_at_month,
        enrolled_at_month,
        grade_status_avg_first_enrolled_at_month,
        second_enrolled_at_month,
        enrolled_at_month_dense_rank,
        enrollment_count,
        min_enrollment_score,
        avg_enrollment_score
    FROM profiled
    WHERE rn <= 5
      AND course_id <= 8
)
SELECT *
FROM (
    SELECT enrolled_at_month, grade_status, second_enrolled_at_month
    FROM top_per_group
) src
PIVOT (
    MIN(second_enrolled_at_month) AS min_v, COUNT(second_enrolled_at_month) AS n
    FOR grade_status IN ('WITHDRAWN', 'INCOMPLETE', 'FAIL')
)
ORDER BY enrolled_at_month
