WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        QUARTER(enrolled_at) AS enrolled_at_quarter,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE course_id >= 2
      AND score <= 78.8
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_grade_status_by_enrolled_at AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_cume_dist,
        SUM(score) OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at_quarter DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_sum_7,
        LAG(enrolled_at, 3) OVER w_enrolled_at_month_by_enrolle AS prev3_enrolled_at,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_id, grade_status)) AS distinct_student_id_in_course_id_grade_status,
        FIRST_VALUE(enrolled_at_quarter) OVER (PARTITION BY course_id, enrolled_at_quarter ORDER BY enrolled_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_quarter,
        COUNT(*) OVER (PARTITION BY term, grade_status) AS rows_in_term_grade_status,
        ROUND(AVG(score) OVER (PARTITION BY grade_status ORDER BY enrolled_at_quarter ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7
    FROM filtered_enrollments
    WINDOW w_grade_status_by_enrolled_at AS (PARTITION BY grade_status ORDER BY enrolled_at_quarter ASC NULLS LAST, id), w_enrolled_at_month_by_enrolle AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY enrolled_at DESC, id)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(enrolled_at_quarter - AVG(enrolled_at_quarter) OVER (PARTITION BY first_enrolled_at_quarter), 2) AS enrolled_at_quarter_vs_first_enrolled_at_quarter,
        ROUND(enrolled_at_cume_dist - AVG(enrolled_at_cume_dist) OVER w_first_enrolled_at_quarter, 2) AS enrolled_at_cume_dist_vs_first_enrolled_at_quart,
        MIN(score) OVER (PARTITION BY term, student_id ORDER BY distinct_student_id_in_course_id_grade_status DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        SIZE(COLLECT_SET(term) OVER w_first_enrolled_at_quarter) AS distinct_term_in_first_enrolled_at_quarter,
        NTH_VALUE(rows_in_term_grade_status, 2) OVER (PARTITION BY YEAR(prev3_enrolled_at) ORDER BY rows_in_term_grade_status, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_term_grade_status
    FROM with_trends
    WINDOW w_first_enrolled_at_quarter AS (PARTITION BY first_enrolled_at_quarter)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    enrolled_at_quarter,
    grade_status_ref,
    rn,
    enrolled_at_cume_dist,
    prev3_enrolled_at,
    distinct_student_id_in_course_id_grade_status,
    first_enrolled_at_quarter,
    rows_in_term_grade_status,
    enrolled_at_quarter_vs_first_enrolled_at_quarter,
    enrolled_at_cume_dist_vs_first_enrolled_at_quart,
    min_score_so_far,
    distinct_term_in_first_enrolled_at_quarter,
    second_rows_in_term_grade_status,
    ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev3_enrolled_at)) / 60, 1) AS minutes_prev3_enrolled_at_to_enrolled_at
FROM windowed
ORDER BY grade_status_ref
