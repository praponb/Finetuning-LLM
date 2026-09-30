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
        enrolled_at + INTERVAL 7 DAYS AS enrolled_at_plus_7d,
        CONCAT_WS(': ', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
      AND course_id IS NOT NULL
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at_plus_7d,
        s.term_grade_status_label,
        c.level AS course_level,
        c.course_code,
        st.student_name
    FROM recent_enrollments s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        course_level,
        student_name,
        MAX(score) OVER (ORDER BY enrolled_at_plus_7d, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_max_4,
        LAG(score) OVER (ORDER BY enrolled_at_plus_7d, id) AS prev_score,
        DENSE_RANK() OVER (ORDER BY enrolled_at_plus_7d, id) AS enrolled_at_plus_7d_dense_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at_plus_7d), 3) AS enrolled_at_plus_7d_percent_rank,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id
    FROM with_details
)
-- Subtotals with ROLLUP
SELECT
    student_id,
    course_id,
    grade_status,
    COUNT(DISTINCT course_id) AS distinct_course_ids,
    ROUND(AVG(score_max_4), 2) AS avg_score_max_4,
    SUM(CASE WHEN LENGTH(TRIM(student_name)) >= 5 THEN enrolled_at_plus_7d_percent_rank ELSE 0 END) AS enrolled_at_plus_7d_percent_rank_length_trim_stu
FROM sequenced
GROUP BY ROLLUP(student_id, course_id, grade_status)
ORDER BY student_id NULLS LAST, course_id NULLS LAST, grade_status NULLS LAST
