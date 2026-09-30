WITH base_enrollments AS (
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
    WHERE grade_status = 'PASS'
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at
    FROM base_enrollments s
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        last_enrollment_enrolled_at,
        ROUND(AVG(score) OVER (PARTITION BY term, course_id ORDER BY last_enrollment_enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS score_avg_3,
        MIN(score) OVER (PARTITION BY student_id, MONTH(last_enrollment_enrolled_at)) AS student_id_last_enrollment_enrolled_at_month_min,
        MAX(score) OVER (PARTITION BY student_id, MONTH(last_enrollment_enrolled_at)) AS student_id_last_enrollment_enrolled_at_month_max,
        score - LAG(score) OVER (PARTITION BY term, course_id ORDER BY last_enrollment_enrolled_at DESC, id) AS score_change,
        LEAD(score, 3, 0) OVER (PARTITION BY term, course_id ORDER BY last_enrollment_enrolled_at DESC, id) AS next3_score
    FROM summarized_rows
),
with_parent_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.student_id_last_enrollment_enrolled_at_month_min,
        s.student_id_last_enrollment_enrolled_at_month_max,
        c.course_code
    FROM metrics s
    JOIN courses c ON c.id = s.course_id
    WHERE UPPER(TRIM(c.course_code)) LIKE 'C%'
)
SELECT
    term,
    course_id,
    COUNT(DISTINCT grade_status) AS distinct_grade_status,
    MAX(student_id_last_enrollment_enrolled_at_month_max) AS max_student_id_last_enrollment_enrolled_at_month
FROM with_parent_course
GROUP BY GROUPING SETS ((term, course_id), (term), (course_id))
