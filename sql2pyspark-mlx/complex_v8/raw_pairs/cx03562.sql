WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE (course_id >= 2 OR student_id <= 2)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        MAX(score) AS max_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        e.max_enrollment_score
    FROM scoped s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        grade_status_len,
        last_enrollment_enrolled_at,
        max_enrollment_score,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        MIN(grade_status_len) OVER (PARTITION BY grade_status, student_id) AS grade_status_student_id_min_grade_status_len
    FROM with_stats
),
enriched_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.course_id,
        s.grade_status_len,
        s.max_enrollment_score,
        s.grade_status_student_id_min_grade_status_len,
        c.level AS course_level,
        c.course_code,
        c.dept AS course_dept
    FROM with_trends s
    JOIN courses c ON c.id = s.course_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(': ', course_level, course_dept) AS course_level_course_dept_label,
        UPPER(TRIM(course_level)) AS course_level_clean,
        COALESCE(grade_status_len, 0) AS grade_status_len_filled
    FROM enriched_course
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM tagged s
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_id,
        s.max_enrollment_score,
        s.course_level,
        s.course_dept,
        s.course_level_course_dept_label,
        s.course_level_clean,
        s.enrollment_count,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score
    FROM summarized_rows s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
)
-- Final projection
SELECT
    id,
    course_id,
    max_enrollment_score,
    course_level_course_dept_label,
    course_level_clean,
    enrollment_count,
    enrollment_id,
    enrollment_grade_status,
    enrollment_score,
    SUBSTRING(TRIM(course_dept), 1, 3) AS course_dept_prefix
FROM detail_rows
ORDER BY max_enrollment_score DESC, id
