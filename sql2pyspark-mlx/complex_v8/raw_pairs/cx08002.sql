WITH valid_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        LENGTH(TRIM(dept)) AS dept_len,
        CASE WHEN level IN ('advanced', 'intro') THEN 'group_a' WHEN level = 'core' THEN 'group_b' ELSE 'other' END AS level_group,
        ABS(credits - 2) AS credits_distance
    FROM courses
    WHERE UPPER(TRIM(course_code)) LIKE 'C%'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        dept,
        dept_len,
        level_group,
        ROW_NUMBER() OVER (ORDER BY credits, id) AS credits_seq,
        LAST_VALUE(dept_len) IGNORE NULLS OVER (ORDER BY credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_len,
        LEAD(dept_len, 1, 0) OVER (ORDER BY credits, id) AS next_dept_len
    FROM valid_courses
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.dept,
        s.level_group,
        s.last_known_dept_len,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term
    FROM with_windows s
    JOIN enrollments e ON e.course_id = s.id AND e.student_id <= 9
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM exploded_detail_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.term <> '2025T2'
              AND e.score > 5.7
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(enrollment_enrolled_at) - UNIX_TIMESTAMP(LAG(enrollment_enrolled_at) OVER (PARTITION BY last_known_dept_len, dept ORDER BY last_known_dept_len, id, enrollment_id))) / 60, 1) AS minutes_since_prev_enrollment_enrolled_at,
        ROW_NUMBER() OVER w_last_known_dept_len_dept_by AS last_known_dept_len_seq
    FROM with_activity
    WINDOW w_last_known_dept_len_dept_by AS (PARTITION BY last_known_dept_len, dept ORDER BY last_known_dept_len, id, enrollment_id)
)
SELECT
    id,
    dept,
    level_group,
    last_known_dept_len,
    enrollment_id,
    enrollment_student_id,
    enrollment_enrolled_at,
    enrollment_term,
    minutes_since_prev_enrollment_enrolled_at
FROM metrics
ORDER BY enrollment_enrolled_at DESC
