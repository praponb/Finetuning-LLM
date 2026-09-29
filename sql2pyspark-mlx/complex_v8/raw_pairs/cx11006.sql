WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', term, grade_status) AS term_grade_status_label,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-13 09:00:00'
),
latest AS (
    -- Keep the latest row per grade_status, term
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        term_grade_status_label,
        enrolled_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status, term ORDER BY enrolled_at DESC, id DESC) AS recency_rank
        FROM scoped_enrollments
    ) ranked
    WHERE recency_rank = 1
),
with_parent_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_label,
        c.level AS course_level
    FROM latest s
    JOIN courses c ON c.id = s.course_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        enrolled_at,
        course_level,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        LEAD(enrolled_at, 2) OVER (ORDER BY enrolled_at, id) AS next2_enrolled_at,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY enrolled_at DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at
    FROM with_parent_course
),
rollup_base AS (
    -- Group-level totals
    SELECT
        term,
        student_id,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        ARRAY_SORT(COLLECT_SET(course_level)) AS course_level_list,
        COUNT(*) AS row_count,
        MIN(next2_enrolled_at) AS first_next2_enrolled_at,
        SUM(CASE WHEN term IN ('2024T2', '2025T1', '2025T2') THEN 1 ELSE 0 END) AS n_term_in_2024t2_2025t1_2025t2
    FROM with_trends
    WHERE TO_DATE(first_known_enrolled_at) < DATE '2025-02-24'
    GROUP BY term, student_id, DATE_TRUNC('MONTH', enrolled_at)
    HAVING COUNT(*) >= 3
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(row_count, 2) OVER (ORDER BY row_count NULLS LAST, term, student_id, enrolled_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_row_count,
        SUM(row_count) OVER (ORDER BY row_count DESC, term, student_id, enrolled_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_count,
        MAX(row_count) OVER w_row_count AS row_count_max_row_count,
        ROUND(row_count / NULLIF(SUM(row_count) OVER (PARTITION BY row_count), 0), 4) AS row_count_share_of_row_count,
        LAG(enrolled_at_month) OVER w_all_by_row_count AS prev_enrolled_at_month,
        LAST_VALUE(row_count) OVER (ORDER BY row_count NULLS LAST, term, student_id, enrolled_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_row_count
    FROM rollup_base
    WINDOW w_row_count AS (PARTITION BY row_count), w_all_by_row_count AS (ORDER BY row_count NULLS LAST, term, student_id, enrolled_at_month)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics
    WHERE second_row_count > (SELECT MIN(second_row_count) FROM metrics)
)
-- Final projection
SELECT
    term,
    student_id,
    enrolled_at_month,
    row_count,
    first_next2_enrolled_at,
    second_row_count,
    running_row_count,
    row_count_max_row_count,
    row_count_share_of_row_count,
    prev_enrolled_at_month,
    last_row_count
FROM screened
ORDER BY term DESC, running_row_count DESC, student_id, enrolled_at_month
LIMIT 5
