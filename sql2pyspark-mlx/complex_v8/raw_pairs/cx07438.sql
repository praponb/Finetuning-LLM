WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref
    FROM enrollments
    WHERE score >= 32.7
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(score) OVER (PARTITION BY course_id ORDER BY enrolled_at_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_sum_1_prior,
        DENSE_RANK() OVER (PARTITION BY term, MONTH(enrolled_at_date) ORDER BY score DESC NULLS FIRST) AS score_dense_rank,
        NTILE(4) OVER (PARTITION BY term, MONTH(enrolled_at_date) ORDER BY score DESC NULLS FIRST, id) AS score_quartile,
        COUNT(*) OVER (PARTITION BY course_id, YEAR(enrolled_at_date)) AS rows_in_course_id_enrolled_at_date_year,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_id ORDER BY enrolled_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND(AVG(score) OVER w_course_id_enrolled_at_date_y, 2) AS course_id_enrolled_at_date_year_avg_score
    FROM cleaned_enrollments
    WINDOW w_course_id_enrolled_at_date_y AS (PARTITION BY course_id, YEAR(enrolled_at_date))
)
-- Subtotals with GROUPING SETS
SELECT
    course_id,
    student_id,
    score_dense_rank,
    GROUPING(course_id) AS is_total_course_id,
    ROUND(SUM(course_id_enrolled_at_date_year_avg_score) / NULLIF(SUM(rows_in_course_id_enrolled_at_date_year), 0), 3) AS course_id_enrolled_at_date_year_avg_score_to_row,
    MAX(score) AS max_score
FROM with_windows
GROUP BY GROUPING SETS ((course_id, student_id, score_dense_rank), (course_id))
