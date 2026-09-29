WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        LENGTH(TRIM(term)) AS term_len,
        ABS(score - 32.7) AS score_distance
    FROM enrollments
    WHERE course_id <= 10
),
grade_status_stats AS (
    -- Avg term_len per grade_status
    SELECT grade_status, AVG(term_len) AS avg_term_len
    FROM recent_enrollments
    GROUP BY grade_status
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_term_len,
        ROUND(s.term_len - g.avg_term_len, 2) AS term_len_minus_avg
    FROM recent_enrollments s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.term_len > g.avg_term_len
),
joined_student AS (
    -- Attach student attributes
    SELECT
        s.*,
        st.is_international AS student_is_international,
        st.enrolled_on AS student_enrolled_on,
        st.major AS student_major,
        c.level AS course_level
    FROM compared s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
    WHERE st.enrolled_on < DATE '2025-08-27'
)
SELECT
    COALESCE(student_major, 'ALL') AS student_major,
    student_is_international,
    COALESCE(term, 'ALL') AS term,
    MAX(term_len) AS max_term_len,
    ARRAY_SORT(COLLECT_SET(student_major)) AS student_major_list
FROM joined_student
GROUP BY GROUPING SETS ((student_major, student_is_international, term), (student_major))
