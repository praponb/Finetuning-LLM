WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month
    FROM enrollments
    WHERE grade_status IN ('INCOMPLETE', 'PASS')
      AND student_id IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(score) OVER (PARTITION BY term, student_id ORDER BY enrolled_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_sum_7,
        ROUND(score / NULLIF(SUM(score) OVER w_enrolled_at_month_student_id, 0), 4) AS score_share_of_enrolled_at_month_student_id
    FROM filtered_enrollments
    WINDOW w_enrolled_at_month_student_id AS (PARTITION BY MONTH(enrolled_at), student_id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('WEEK', enrolled_at_month) AS enrolled_at_month_week,
        LENGTH(TRIM(term)) AS term_len,
        LEAST(score, score_sum_7) AS least_score_score_sum_7
    FROM ranked
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at_month,
    score_sum_7,
    score_share_of_enrolled_at_month_student_id,
    enrolled_at_month_week,
    term_len,
    least_score_score_sum_7,
    ROUND(score * 1.07, 2) AS score_adj
FROM annotated
