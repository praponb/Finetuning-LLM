WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE grade_status IS NOT NULL
      AND course_id IS NOT NULL
),
grade_status_stats AS (
    -- Avg term_len per grade_status
    SELECT grade_status, AVG(term_len) AS avg_term_len
    FROM base
    GROUP BY grade_status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_term_len,
        ROUND(s.term_len - g.avg_term_len, 2) AS term_len_minus_avg
    FROM base s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.term_len > g.avg_term_len
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.term_len,
        s.term_len_minus_avg,
        c.course_code,
        c.dept AS course_dept
    FROM outliers s
    JOIN courses c ON c.id = s.course_id AND c.level = 'intro'
    WHERE c.dept = 'CS'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        score,
        grade_status,
        enrolled_at,
        term_len,
        term_len_minus_avg,
        course_dept,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label,
        COALESCE(term_len_minus_avg, 0.0) AS term_len_minus_avg_filled
    FROM joined_course
)
-- Subtotals with GROUPING SETS
SELECT
    grade_status,
    term_len,
    course_dept,
    MIN(term_len_minus_avg) AS min_term_len_minus_avg,
    COUNT(*) AS row_count,
    COUNT(DISTINCT grade_status) AS distinct_grade_status
FROM labeled
GROUP BY GROUPING SETS ((grade_status, term_len, course_dept), (grade_status), ())
ORDER BY grade_status NULLS LAST, term_len NULLS LAST, course_dept NULLS LAST
