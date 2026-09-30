WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term IS NOT NULL
      AND grade_status IN ('INCOMPLETE', 'PASS')
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM cleaned_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term = '2024T1'
        )
),
qualified AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM without_matches s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND UPPER(TRIM(c.course_code)) LIKE 'A%'
              AND c.level = 'intro'
        )
),
with_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        st.enrolled_on AS student_enrolled_on,
        c.dept AS course_dept
    FROM qualified s
    JOIN students st ON st.id = s.student_id AND LOWER(st.student_name) LIKE '%ir%'
    LEFT JOIN courses c ON c.id = s.course_id
    WHERE st.enrolled_on < DATE '2025-07-03'
),
qualified_2 AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status = 'PASS'
              AND e.enrolled_at >= TIMESTAMP '2025-01-13 09:00:00'
        )
),
totals AS (
    -- Summarise per group
    SELECT
        course_dept,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        SUM(score) FILTER (WHERE course_dept IN ('Biz', 'Bio', 'CS')) AS score_course_dept_in_biz_bio_cs,
        PERCENTILE_APPROX(score, 0.5) AS median_score,
        ROUND(SUM(score), 2) AS total_score,
        DATEDIFF(MAX(student_enrolled_on), MIN(student_enrolled_on)) AS student_enrolled_on_span_days
    FROM qualified_2
    GROUP BY 1, 2
),
without_matches_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE total_score < (SELECT MAX(total_score) FROM totals)
)
-- Final projection
SELECT course_dept, enrolled_at_month, score_course_dept_in_biz_bio_cs
FROM without_matches_2
ORDER BY course_dept NULLS LAST
