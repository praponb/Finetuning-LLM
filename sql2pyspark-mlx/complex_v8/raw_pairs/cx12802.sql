WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score > 42.4
),
with_parent AS (
    -- Attach course attributes
    SELECT
        s.*,
        c.level AS course_level,
        c.dept AS course_dept,
        c.course_code
    FROM recent_enrollments s
    JOIN courses c ON c.id = s.course_id
),
with_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.course_level,
        s.course_code,
        st.student_name,
        c.course_code AS course_course_code,
        c.dept AS course_dept_2
    FROM with_parent s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_details_student s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
        )
      AND s.course_dept_2 IS NOT NULL
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(course_code)) AS course_code_len,
        CASE WHEN score >= 57.3 THEN 'A' WHEN score >= 26.2 THEN 'B' ELSE 'C' END AS score_tier,
        CASE WHEN score >= 73.0 THEN 'A' WHEN score >= 20.6 THEN 'B' ELSE 'C' END AS score_tier_2
    FROM screened
),
course_level_stats AS (
    -- Median course_code_len per course_level
    SELECT course_level, PERCENTILE_APPROX(course_code_len, 0.5) AS median_course_code_len
    FROM scored
    GROUP BY course_level
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_course_code_len,
        ROUND(s.course_code_len - g.median_course_code_len, 2) AS course_code_len_minus_median
    FROM scored s
    JOIN course_level_stats g ON g.course_level = s.course_level
    WHERE s.course_code_len >= g.median_course_code_len
)
-- Final projection
SELECT
    id,
    course_id,
    grade_status,
    course_code,
    student_name,
    course_dept_2,
    score_tier,
    score_tier_2,
    median_course_code_len
FROM above_average
WHERE score_tier_2 <> 'B'
ORDER BY grade_status
