WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE term = '2025T2'
),
combined_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        c.level AS course_level,
        c.credits AS course_credits,
        c.dept AS course_dept,
        st.student_name,
        st.major AS student_major
    FROM recent_enrollments s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
    WHERE c.dept IN ('Art', 'Biz', 'CS')
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM combined_details_course s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score > 38.1
              AND e.student_id <= 2
        )
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN e.grade_status <> 'WITHDRAWN' THEN 1 END) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_grade_status_count
    FROM with_activity s
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        course_level,
        student_name,
        student_major,
        enrollment_grade_status_count,
        SIZE(COLLECT_SET(course_dept) OVER (PARTITION BY course_id, course_level)) AS distinct_course_dept_in_course_id_course_level,
        ROUND(PERCENT_RANK() OVER (PARTITION BY enrollment_grade_status_count ORDER BY enrolled_at DESC), 3) AS enrolled_at_percent_rank,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER w_course_id_course_level, 0), 4) AS course_credits_share_of_course_id_course_level
    FROM with_stats
    WINDOW w_course_id_course_level AS (PARTITION BY course_id, course_level)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.course_level,
        s.student_name,
        s.student_major,
        s.enrollment_grade_status_count,
        s.enrolled_at_percent_rank,
        s.course_credits_share_of_course_id_course_level,
        c.dept AS course_dept,
        c.course_code,
        c.level AS course_level_2,
        st.is_international AS student_is_international,
        st.major AS student_major_2
    FROM ranked s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_details s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    course_level,
    student_name,
    student_major,
    enrollment_grade_status_count,
    enrolled_at_percent_rank,
    course_credits_share_of_course_id_course_level,
    course_dept,
    course_level_2,
    student_major_2,
    HOUR(enrolled_at) AS enrolled_at_hour
FROM qualified
ORDER BY student_name DESC NULLS FIRST
