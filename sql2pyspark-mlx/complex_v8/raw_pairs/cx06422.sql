WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LENGTH(TRIM(term)) AS term_len,
        LENGTH(TRIM(term)) AS term_len_2,
        ABS(score - 44.4) AS score_distance
    FROM enrollments
    WHERE score BETWEEN 13.9 AND 85.2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        score,
        term_len_2,
        score_distance,
        NTILE(10) OVER w_all_by_term_len_2 AS term_len_2_decile,
        ROUND(term_len / NULLIF(SUM(term_len) OVER (PARTITION BY term_len, course_id), 0), 4) AS term_len_share_of_term_len_course_id,
        MIN(term_len_2) OVER (PARTITION BY term_len, course_id) AS term_len_course_id_min_term_len_2
    FROM filtered_enrollments
    WINDOW w_all_by_term_len_2 AS (ORDER BY term_len_2, id)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(score_distance) OVER (ORDER BY term_len_share_of_term_len_course_id ASC, id) AS prev_score_distance,
        SUM(term_len_2_decile) OVER (PARTITION BY course_id, student_id) AS course_id_student_id_sum_term_len_2_decile
    FROM metrics
)
-- Final projection
SELECT
    id,
    course_id,
    score,
    term_len_2,
    term_len_share_of_term_len_course_id,
    term_len_course_id_min_term_len_2,
    prev_score_distance,
    course_id_student_id_sum_term_len_2_decile
FROM ranked
ORDER BY term_len_course_id_min_term_len_2, prev_score_distance, id
LIMIT 10
