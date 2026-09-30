WITH prepared AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CASE WHEN dept IN ('Biz', 'CS') THEN 'group_a' WHEN dept = 'Bio' THEN 'group_b' ELSE 'other' END AS dept_group,
        LENGTH(TRIM(dept)) AS dept_len
    FROM courses
    WHERE credits BETWEEN 2 AND 4
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM prepared
    WHERE dept_len < (SELECT MAX(dept_len) FROM prepared)
),
tagged AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(course_code)) AS course_code_len
    FROM without_matches
    WHERE level NOT IN ('intro', 'core')
),
dept_stats AS (
    -- Avg dept_len per dept
    SELECT dept, AVG(dept_len) AS avg_dept_len
    FROM tagged
    GROUP BY dept
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_dept_len,
        ROUND(s.dept_len - g.avg_dept_len, 2) AS dept_len_minus_avg
    FROM tagged s
    JOIN dept_stats g ON g.dept = s.dept
    WHERE s.dept_len > g.avg_dept_len
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        SUM(score) AS total_enrollment_score,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.total_enrollment_score,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM above_average s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(course_code_len / NULLIF(avg_dept_len, 0), 3) AS course_code_len_per_avg_dept_len,
        ABS(dept_len - 7) AS dept_len_distance
    FROM with_counts
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM annotated
    WHERE total_enrollment_score > (SELECT AVG(total_enrollment_score) FROM annotated)
)
SELECT
    id,
    course_code,
    dept,
    level,
    dept_len,
    dept_len_minus_avg,
    total_enrollment_score,
    enrollment_count,
    dept_len_distance,
    FLOOR(dept_len / 2) * 2 AS dept_len_bucket
FROM with_activity
WHERE course_code_len_per_avg_dept_len <= 6.0
ORDER BY total_enrollment_score
