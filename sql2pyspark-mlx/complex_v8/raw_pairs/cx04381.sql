WITH scoped AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
    WHERE level NOT IN ('advanced', 'core')
      AND UPPER(TRIM(course_code)) LIKE 'C%'
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.dept,
        s.level,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score
    FROM scoped s
    LEFT JOIN enrollments e ON e.course_id = s.id
),
with_details_enrollment AS (
    -- Join enrollments
    SELECT s.*, e.enrolled_at AS enrollment_enrolled_at_2
    FROM line_level s
    LEFT JOIN enrollments e ON e.id = s.enrollment_id AND e.grade_status = 'PASS'
),
joined_enrollment AS (
    -- Join enrollments
    SELECT
        s.id,
        s.dept,
        s.level,
        s.enrollment_id,
        s.enrollment_student_id,
        s.enrollment_enrolled_at,
        s.enrollment_grade_status,
        s.enrollment_score,
        s.enrollment_enrolled_at_2,
        e.enrolled_at AS enrollment_enrolled_at_3,
        e.student_id AS enrollment_student_id_2,
        st.gpa AS student_gpa
    FROM with_details_enrollment s
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN students st ON st.id = e.student_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(enrollment_score) OVER (PARTITION BY enrollment_id ORDER BY student_gpa DESC, id, enrollment_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS enrollment_score_avg_2_prior,
        ROUND(enrollment_score / NULLIF(SUM(enrollment_score) OVER (PARTITION BY dept, enrollment_grade_status), 0), 4) AS enrollment_score_share_of_dept_enrollment_grade_,
        SUM(enrollment_score) OVER (PARTITION BY dept, enrollment_grade_status) AS dept_enrollment_grade_status_sum_enrollment_scor
    FROM joined_enrollment
),
dept_stats AS (
    -- Avg enrollment_score_avg_2_prior per dept
    SELECT dept, AVG(enrollment_score_avg_2_prior) AS avg_enrollment_score_avg_2_prior
    FROM ranked
    GROUP BY dept
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_enrollment_score_avg_2_prior,
        ROUND(s.enrollment_score_avg_2_prior - g.avg_enrollment_score_avg_2_prior, 2) AS enrollment_score_avg_2_prior_minus_avg
    FROM ranked s
    JOIN dept_stats g ON g.dept = s.dept
    WHERE s.enrollment_score_avg_2_prior > g.avg_enrollment_score_avg_2_prior
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(dept, 'ALL') AS dept,
    COALESCE(enrollment_grade_status, 'ALL') AS enrollment_grade_status,
    GROUPING(dept) AS is_total_dept,
    MIN(enrollment_score) AS min_enrollment_score,
    MAX(enrollment_enrolled_at) AS last_enrollment_enrolled_at,
    MAX(dept_enrollment_grade_status_sum_enrollment_scor) AS max_dept_enrollment_grade_status_sum_enrollment_
FROM outliers
GROUP BY ROLLUP(dept, enrollment_grade_status)
ORDER BY dept NULLS LAST, enrollment_grade_status NULLS LAST
