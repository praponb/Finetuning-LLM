WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE course_id >= 7
      AND score > 8.7
),
with_parent_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.term_slug,
        st.is_international AS student_is_international,
        st.major AS student_major,
        st.student_name
    FROM cleaned_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_parent_student s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score BETWEEN 8.8 AND 76.8
        )
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.student_is_international,
        s.student_major,
        s.student_name,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score
    FROM without_matches s
    LEFT JOIN enrollments e ON e.student_id = s.student_id AND e.grade_status = 'WITHDRAWN'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.student_is_international,
        s.student_major,
        s.student_name,
        s.enrollment_id,
        s.enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM exploded_detail_enrollments s
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
),
with_parent AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.student_is_international,
        s.student_major,
        s.student_name,
        s.enrollment_id,
        s.enrollment_course_id,
        s.enrollment_enrolled_at,
        c.level AS course_level,
        c.credits AS course_credits
    FROM combined_details s
    JOIN courses c ON c.id = s.course_id
),
enrollment_course_id_stats AS (
    -- Avg course_credits per enrollment_course_id
    SELECT enrollment_course_id, AVG(course_credits) AS avg_course_credits
    FROM with_parent
    GROUP BY enrollment_course_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_course_credits,
        ROUND(s.course_credits - g.avg_course_credits, 2) AS course_credits_minus_avg
    FROM with_parent s
    JOIN enrollment_course_id_stats g ON g.enrollment_course_id = s.enrollment_course_id
    WHERE s.course_credits > g.avg_course_credits
)
SELECT
    id,
    student_id,
    student_major,
    student_name,
    enrollment_id,
    enrollment_course_id,
    enrollment_enrolled_at,
    course_level,
    course_credits,
    avg_course_credits,
    course_credits_minus_avg
FROM above_average
WHERE enrollment_course_id IS NOT NULL
ORDER BY course_level, course_credits_minus_avg NULLS LAST
