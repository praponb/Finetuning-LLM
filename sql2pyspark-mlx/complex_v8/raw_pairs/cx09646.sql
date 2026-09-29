WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 9 AND 14
),
course_id_stats AS (
    -- Avg score_filled per course_id
    SELECT course_id, AVG(score_filled) AS avg_score_filled
    FROM cleaned
    GROUP BY course_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_score_filled,
        ROUND(s.score_filled - g.avg_score_filled, 2) AS score_filled_minus_avg
    FROM cleaned s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score_filled > g.avg_score_filled
),
without_matches AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM outliers s
    WHERE EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.cohort = 'C2023'
        )
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.course_id = s.course_id) AS avg_enrollment_score,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS total_enrollment_score
    FROM without_matches s
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    course_id,
    student_id,
    SUM(CASE WHEN grade_status <> 'PASS' THEN total_enrollment_score ELSE 0 END) AS total_enrollment_score_grade_status_pass,
    ROUND(AVG(avg_enrollment_score), 2) AS avg_avg_enrollment_score,
    COUNT(*) AS cnt
FROM with_counts
GROUP BY GROUPING SETS ((grade_status, course_id, student_id), (grade_status))
ORDER BY grade_status NULLS LAST, course_id NULLS LAST, student_id NULLS LAST
