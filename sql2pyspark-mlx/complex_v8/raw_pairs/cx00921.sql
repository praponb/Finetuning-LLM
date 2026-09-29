WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len,
        ROUND(score * 0.25, 2) AS score_adj,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE score > 16.3
),
enriched AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.term_len,
        s.score_adj,
        s.days_since_enrolled_at,
        c.course_code
    FROM filtered_enrollments s
    JOIN courses c ON c.id = s.course_id
    WHERE UPPER(TRIM(c.course_code)) LIKE 'C%'
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.score_adj,
        s.days_since_enrolled_at,
        s.course_code,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM enriched s
    JOIN enrollments e ON e.course_id = s.course_id
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.course_code,
        s.enrollment_id,
        s.enrollment_student_id,
        e.id AS enrollment_id_2,
        e.student_id AS enrollment_student_id_2,
        e.grade_status AS enrollment_grade_status_2
    FROM line_level_enrollments s
    JOIN enrollments e ON e.course_id = s.course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.enrollment_student_id_2) AS max_enrollment_score,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.enrollment_student_id_2) AS last_enrollment_enrolled_at
    FROM detail_rows_enrollments s
),
with_details AS (
    -- Attach enrollment attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.course_code,
        s.enrollment_id,
        s.enrollment_student_id,
        s.enrollment_id_2,
        s.enrollment_student_id_2,
        s.enrollment_grade_status_2,
        s.max_enrollment_score,
        s.last_enrollment_enrolled_at,
        e.student_id AS enrollment_student_id_3,
        c.credits AS course_credits,
        c.level AS course_level
    FROM summarized_rows s
    JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN courses c ON c.id = s.course_id
),
enrollment_stats_2 AS (
    -- Per-course enrollments metrics
    SELECT course_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM with_details s
    LEFT JOIN enrollment_stats_2 e ON e.course_id = s.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    course_code,
    enrollment_id,
    enrollment_student_id,
    enrollment_id_2,
    enrollment_student_id_2,
    max_enrollment_score,
    course_credits,
    CASE WHEN course_level IN ('core', 'intro') THEN 'group_a' WHEN course_level = 'advanced' THEN 'group_b' ELSE 'other' END AS course_level_group,
    GREATEST(course_credits, max_enrollment_score) AS greatest_course_credits_max_enrollment_score
FROM with_counts
WHERE max_enrollment_score >= 30.0
