WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE (grade_status IN ('PASS', 'WITHDRAWN') OR course_id IS NOT NULL)
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS max_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM scoped_enrollments s
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, c.dept AS course_dept
    FROM with_stats s
    JOIN courses c ON c.id = s.course_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_ym,
        ROUND(AVG(max_enrollment_score), 2) AS avg_max_enrollment_score,
        COUNT(*) AS n_rows,
        MIN(enrollment_count) AS min_enrollment_count,
        MAX(enrolled_at_date) AS last_enrolled_at_date,
        COUNT(*) FILTER (WHERE term = '2024T2') AS n_term_2024t2,
        ROUND(STDDEV(enrollment_count), 2) AS stddev_enrollment_count
    FROM with_parent
    GROUP BY DATE_FORMAT(enrolled_at, 'yyyy-MM')
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT DISTINCT n_term_2024t2, n_rows
FROM rollup_base
ORDER BY n_term_2024t2
