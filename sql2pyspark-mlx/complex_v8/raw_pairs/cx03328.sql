WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label,
        CASE WHEN grade_status IN ('INCOMPLETE', 'FAIL') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE term IN ('2025T2', '2025T1')
      AND course_id IS NOT NULL
),
enriched AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.enrolled_at_label,
        s.grade_status_group,
        c.dept AS course_dept,
        c.level AS course_level
    FROM prepared_enrollments s
    JOIN courses c ON c.id = s.course_id AND UPPER(TRIM(c.course_code)) LIKE 'C%'
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(CASE WHEN term = '2024T2' THEN 1 END) AS enrollment_term_count,
        MIN(score) AS min_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count,
        e.min_enrollment_score
    FROM enriched s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
grade_status_group_stats AS (
    -- Avg enrollment_term_count per grade_status_group
    SELECT grade_status_group, AVG(enrollment_term_count) AS avg_enrollment_term_count
    FROM profiled
    GROUP BY grade_status_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_enrollment_term_count,
        ROUND(s.enrollment_term_count - g.avg_enrollment_term_count, 2) AS enrollment_term_count_minus_avg
    FROM profiled s
    JOIN grade_status_group_stats g ON g.grade_status_group = s.grade_status_group
    WHERE s.enrollment_term_count > g.avg_enrollment_term_count
)
SELECT
    id,
    student_id,
    score,
    enrolled_at,
    enrolled_at_label,
    avg_enrollment_term_count,
    min_enrollment_score + enrollment_term_count AS min_enrollment_score_plus_enrollment_term_count,
    COALESCE(score, 0.0) AS score_filled
FROM compared
ORDER BY score_filled NULLS LAST, id
LIMIT 10
