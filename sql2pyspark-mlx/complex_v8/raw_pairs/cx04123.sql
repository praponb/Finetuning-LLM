WITH recent_enrollments AS (
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
    WHERE student_id IS NOT NULL
),
with_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        st.is_international AS student_is_international,
        st.major AS student_major,
        st.gpa AS student_gpa
    FROM recent_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score
    FROM with_details s
    JOIN enrollments e ON e.course_id = s.course_id
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrollment_id,
        s.enrollment_student_id,
        s.enrollment_grade_status,
        e.id AS enrollment_id_2,
        e.score AS enrollment_score_2,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM line_level_enrollments s
    JOIN enrollments e ON e.course_id = s.course_id
),
enriched_enrollment AS (
    -- Join enrollments
    SELECT
        s.*,
        e.grade_status AS enrollment_grade_status_2,
        e.student_id AS enrollment_student_id_2,
        e.term AS enrollment_term_2
    FROM with_children s
    JOIN enrollments e ON e.id = s.enrollment_id AND e.score BETWEEN 8.7 AND 74.9
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrollment_id,
        enrollment_student_id,
        enrollment_id_2,
        enrollment_score_2,
        enrollment_enrolled_at,
        enrollment_grade_status_2,
        enrollment_student_id_2,
        enrollment_term_2,
        ABS(enrollment_score_2 - 57.6) AS enrollment_score_2_distance,
        CASE WHEN enrollment_score_2 >= 66.0 THEN 'HIGH' WHEN enrollment_score_2 >= 20.6 THEN 'MEDIUM' ELSE 'LOW' END AS enrollment_score_2_tier,
        FLOOR(enrollment_score_2 / 25) * 25 AS enrollment_score_2_bucket
    FROM enriched_enrollment
)
-- Final projection
SELECT
    id,
    course_id,
    enrollment_id,
    enrollment_student_id,
    enrollment_score_2,
    enrollment_enrolled_at,
    enrollment_student_id_2,
    enrollment_score_2_distance,
    enrollment_score_2_bucket,
    COALESCE(enrollment_score_2, 0.0) AS enrollment_score_2_filled
FROM enriched
WHERE enrollment_enrolled_at < TIMESTAMP '2025-02-18 11:00:00'
ORDER BY id NULLS LAST, enrollment_score_2_bucket DESC NULLS FIRST
