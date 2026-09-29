WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('WITHDRAWN', 'PASS') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
),
grade_status_stats AS (
    -- Max term_len per grade_status
    SELECT grade_status, MAX(term_len) AS max_term_len
    FROM base_enrollments
    GROUP BY grade_status
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_term_len,
        ROUND(s.term_len - g.max_term_len, 2) AS term_len_minus_max
    FROM base_enrollments s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.term_len = g.max_term_len
)
-- Subtotals with CUBE
SELECT
    grade_status,
    term_len,
    student_id,
    MAX(max_term_len) AS max_max_term_len,
    SUM(term_len) AS total_term_len
FROM compared
GROUP BY CUBE(grade_status, term_len, student_id)
