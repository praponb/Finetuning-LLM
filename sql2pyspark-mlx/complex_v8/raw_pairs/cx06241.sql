WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-24 08:00:00'
      AND student_id <= 2
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.score_filled,
        c.dept AS course_dept,
        c.credits AS course_credits,
        c.level AS course_level,
        st.major AS student_major,
        st.cohort AS student_cohort
    FROM filtered s
    JOIN courses c ON c.id = s.course_id AND c.credits > 2
    JOIN students st ON st.id = s.student_id
    WHERE c.credits > 2
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.student_major,
        s.student_cohort,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at,
        e.course_id AS enrollment_course_id
    FROM with_details_course s
    JOIN enrollments e ON e.student_id = s.student_id AND e.score <= 58.8
)
SELECT
    id,
    student_id,
    course_id,
    student_major,
    student_cohort,
    enrollment_id,
    enrollment_score,
    enrollment_enrolled_at,
    enrollment_course_id,
    DATE_TRUNC('QUARTER', enrollment_enrolled_at) AS enrollment_enrolled_at_quarter,
    COALESCE(student_major, 'UNKNOWN') AS student_major_or_unknown
FROM exploded_detail
ORDER BY enrollment_id DESC
