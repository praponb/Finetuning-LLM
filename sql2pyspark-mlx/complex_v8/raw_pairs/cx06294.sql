WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        ABS(score - 57.8) AS score_distance
    FROM enrollments
    WHERE student_id <= 10
      AND HOUR(enrolled_at) BETWEEN 7 AND 13
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(score_distance) OVER (ORDER BY score_bucket NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_distance_max_2_prior,
        SUM(score_bucket) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_bucket_sum_7,
        MAX(score_distance) OVER (PARTITION BY term) AS term_max_score_distance,
        MIN(score_distance) OVER (PARTITION BY course_id ORDER BY score_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_distance_so_far,
        LAG(score_distance, 3) OVER (PARTITION BY course_id ORDER BY score_distance DESC, id) AS prev3_score_distance,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY grade_status)) AS distinct_student_id_in_grade_status,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY grade_status)) AS distinct_term_in_grade_status,
        FIRST_VALUE(score_bucket) OVER (PARTITION BY course_id ORDER BY score_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score_bucket
    FROM valid_enrollments
)
SELECT
    id,
    term,
    enrolled_at,
    score_bucket,
    score_bucket_sum_7,
    term_max_score_distance,
    min_score_distance_so_far,
    prev3_score_distance,
    distinct_student_id_in_grade_status,
    first_score_bucket
FROM sequenced
ORDER BY term DESC NULLS FIRST
