WITH prepared_enrollments AS (
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
    WHERE term = '2024T2'
      AND student_id IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        ROUND(AVG(score) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        DENSE_RANK() OVER w_course_id_by_score_ties AS score_dense_rank
    FROM prepared_enrollments
    WINDOW w_course_id_by_score_ties AS (PARTITION BY course_id ORDER BY score DESC NULLS FIRST)
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id
    FROM with_windows s
    JOIN enrollments e ON e.course_id = s.course_id
)
SELECT
    id,
    student_id,
    term,
    grade_status,
    enrollment_term,
    enrollment_grade_status
FROM with_children
ORDER BY grade_status, term
