WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE score > 16.3
      AND enrolled_at >= TIMESTAMP '2025-01-31 15:00:00'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(enrolled_at_quarter) OVER w_term AS term_min_enrolled_at_quarter,
        NTILE(3) OVER w_term_by_enrolled_at AS enrolled_at_tercile,
        LEAD(enrolled_at, 3) OVER (ORDER BY enrolled_at, id) AS next3_enrolled_at,
        NTILE(10) OVER w_grade_status_enrolled_at_mon AS enrolled_at_decile,
        ROUND(enrolled_at_quarter / NULLIF(SUM(enrolled_at_quarter) OVER (PARTITION BY student_id, term), 0), 4) AS enrolled_at_quarter_share_of_student_id_term,
        MAX(enrolled_at_quarter) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS enrolled_at_quarter_max_7,
        SUM(enrolled_at_quarter) OVER (PARTITION BY grade_status, MONTH(enrolled_at) ORDER BY enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS enrolled_at_quarter_sum_3,
        DENSE_RANK() OVER (PARTITION BY student_id, enrolled_at_quarter ORDER BY enrolled_at_quarter DESC) AS enrolled_at_quarter_dense_rank,
        LAG(enrolled_at_quarter) OVER (PARTITION BY term ORDER BY enrolled_at ASC, id) AS prev_enrolled_at_quarter,
        SUM(enrolled_at_quarter) OVER (PARTITION BY MONTH(enrolled_at)) AS enrolled_at_month_sum_enrolled_at_quarter,
        LEAD(enrolled_at_quarter, 2) OVER (PARTITION BY grade_status, MONTH(enrolled_at) ORDER BY enrolled_at DESC, id) AS next2_enrolled_at_quarter
    FROM base
    WINDOW w_term AS (PARTITION BY term), w_term_by_enrolled_at AS (PARTITION BY term ORDER BY enrolled_at ASC, id), w_grade_status_enrolled_at_mon AS (PARTITION BY grade_status, MONTH(enrolled_at) ORDER BY enrolled_at DESC, id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM sequenced
    WHERE enrolled_at_quarter_dense_rank <= 3
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(enrolled_at_quarter) OVER (ORDER BY enrolled_at_quarter_max_7 ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS enrolled_at_quarter_avg_4,
        ROW_NUMBER() OVER w_all_by_enrolled_at_quarter_m_2 AS enrolled_at_quarter_max_7_seq,
        next2_enrolled_at_quarter - LAG(next2_enrolled_at_quarter) OVER w_all_by_enrolled_at_quarter_m_2 AS next2_enrolled_at_quarter_change
    FROM top_per_group
    WINDOW w_all_by_enrolled_at_quarter_m_2 AS (ORDER BY enrolled_at_quarter_max_7 ASC NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    enrolled_at,
    enrolled_at_quarter,
    term_min_enrolled_at_quarter,
    next3_enrolled_at,
    enrolled_at_decile,
    enrolled_at_quarter_share_of_student_id_term,
    enrolled_at_quarter_max_7,
    enrolled_at_quarter_sum_3,
    enrolled_at_quarter_dense_rank,
    prev_enrolled_at_quarter,
    enrolled_at_month_sum_enrolled_at_quarter,
    next2_enrolled_at_quarter,
    enrolled_at_quarter_avg_4,
    DATE_FORMAT(next3_enrolled_at, 'MMM yyyy') AS next3_enrolled_at_label,
    DATEDIFF(DATE '2025-06-30', next3_enrolled_at) AS days_since_next3_enrolled_at
FROM with_trends
ORDER BY enrolled_at_quarter_sum_3
