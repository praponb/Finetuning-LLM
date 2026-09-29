WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(score) OVER (PARTITION BY student_id), 2) AS student_id_avg_score,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        MAX(score) OVER (PARTITION BY student_id, term ORDER BY enrolled_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_max_2,
        MAX(score) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        ROUND(AVG(score) OVER (ORDER BY score NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_avg_2_prior,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at
    FROM filtered_enrollments
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.max_score_so_far,
        s.score_avg_2_prior,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id
    FROM windowed s
    JOIN enrollments e ON e.student_id = s.student_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    max_score_so_far,
    score_avg_2_prior,
    enrollment_id,
    enrollment_term,
    enrollment_course_id,
    CONCAT(enrollment_term, '#', CAST(enrollment_id AS STRING)) AS enrollment_term_ref
FROM exploded_detail
ORDER BY enrollment_term_ref DESC, id, enrollment_id
LIMIT 5
