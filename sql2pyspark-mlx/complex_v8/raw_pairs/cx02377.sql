WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', term, grade_status) AS term_grade_status_label,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM enrollments
    WHERE term NOT IN ('2025T1', '2025T2')
),
course_id_stats AS (
    -- Max score per course_id
    SELECT course_id, MAX(score) AS max_score
    FROM base_enrollments
    GROUP BY course_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_score, ROUND(s.score - g.max_score, 2) AS score_minus_max
    FROM base_enrollments s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score = g.max_score
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.course_id = s.course_id) AS avg_enrollment_score
    FROM above_average s
),
qualified AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.gpa < 3.2
              AND YEAR(st.enrolled_on) = 2025
        )
)
SELECT
    student_id,
    term,
    grade_status,
    GROUPING(student_id) AS is_total_student_id,
    MAX(score) AS max_score_2,
    SUM(score) AS total_score
FROM qualified
GROUP BY ROLLUP(student_id, term, grade_status)
ORDER BY student_id NULLS LAST, term NULLS LAST, grade_status NULLS LAST
