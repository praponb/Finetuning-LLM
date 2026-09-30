WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 13:00:00'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_course_id AS rows_in_course_id,
        ROW_NUMBER() OVER (ORDER BY score DESC, id) AS rn,
        LAG(enrolled_at, 2) OVER (ORDER BY score DESC, id) AS prev2_enrolled_at,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        FIRST_VALUE(score) OVER (PARTITION BY term ORDER BY score ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        ROUND(score - AVG(score) OVER w_course_id, 2) AS score_vs_course_id_avg,
        FIRST_VALUE(score) IGNORE NULLS OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        SIZE(COLLECT_SET(grade_status) OVER w_course_id_student_id) AS distinct_grade_status_in_course_id_student_id,
        LAG(score) OVER w_term_by_score AS prev_score,
        NTILE(10) OVER (PARTITION BY term ORDER BY score ASC NULLS LAST, id) AS score_decile,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY term ORDER BY score ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_2,
        COUNT(*) OVER (PARTITION BY course_id, student_id) AS rows_in_course_id_student_id,
        ROUND(AVG(score) OVER w_course_id_student_id, 2) AS course_id_student_id_avg_score
    FROM cleaned_enrollments
    WINDOW w_course_id AS (PARTITION BY course_id), w_course_id_student_id AS (PARTITION BY course_id, student_id), w_term_by_score AS (PARTITION BY term ORDER BY score ASC NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    course_id,
    grade_status,
    enrolled_at,
    rows_in_course_id,
    rn,
    prev2_enrolled_at,
    first_enrolled_at,
    first_score,
    first_known_score,
    prev_score,
    second_enrolled_at_2,
    rows_in_course_id_student_id,
    course_id_student_id_avg_score
FROM with_windows
