WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE course_id <= 3
      AND grade_status IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        ROUND(PERCENT_RANK() OVER w_course_id_grade_status_by_sc, 3) AS score_percent_rank,
        SUM(score) OVER (PARTITION BY course_id, grade_status ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_sum_2
    FROM filtered
    WINDOW w_course_id_grade_status_by_sc AS (PARTITION BY course_id, grade_status ORDER BY score)
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.score_percent_rank,
        s.score_sum_2,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at
    FROM windowed s
    JOIN enrollments e ON e.course_id = s.course_id AND HOUR(e.enrolled_at) BETWEEN 8 AND 14
),
enriched AS (
    -- Join enrollments
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrollment_id,
        s.enrollment_grade_status,
        s.enrollment_enrolled_at,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at_2,
        e.score AS enrollment_score
    FROM with_children s
    JOIN enrollments e ON e.id = s.enrollment_id
    WHERE e.term IS NOT NULL
)
SELECT
    id,
    student_id,
    grade_status,
    enrollment_id,
    enrollment_term,
    enrollment_score,
    FLOOR(enrollment_score / 25) * 25 AS enrollment_score_bucket,
    INITCAP(TRIM(enrollment_term)) AS enrollment_term_clean
FROM enriched
ORDER BY id DESC
