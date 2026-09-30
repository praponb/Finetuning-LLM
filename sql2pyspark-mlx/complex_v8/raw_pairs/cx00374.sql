WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-02-06 11:00:00'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_term_by_enrolled_at_ties, 3) AS enrolled_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY term ORDER BY enrolled_at, id) AS rn
    FROM valid_enrollments
    WINDOW w_term_by_enrolled_at_ties AS (PARTITION BY term ORDER BY enrolled_at)
),
student_id_stats AS (
    -- Avg rn per student_id
    SELECT student_id, AVG(rn) AS avg_rn
    FROM with_trends
    GROUP BY student_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_rn, ROUND(s.rn - g.avg_rn, 2) AS rn_minus_avg
    FROM with_trends s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.rn > g.avg_rn
)
SELECT
    student_id,
    grade_status,
    course_id,
    GROUPING(student_id) AS is_total_student_id,
    ROUND(AVG(enrolled_at_cume_dist), 2) AS avg_enrolled_at_cume_dist,
    COUNT(*) FILTER (WHERE grade_status NOT IN ('FAIL', 'PASS')) AS n_grade_status_not_in_fail_pas,
    COUNT(*) AS cnt
FROM above_average
GROUP BY CUBE(student_id, grade_status, course_id)
ORDER BY student_id NULLS LAST, grade_status NULLS LAST, course_id NULLS LAST
