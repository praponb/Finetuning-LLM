WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        CASE WHEN score >= 78.8 THEN 'A' WHEN score >= 36.6 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE course_id IS NOT NULL
),
joined AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.score_tier,
        c.course_code,
        c.credits AS course_credits,
        st.enrolled_on AS student_enrolled_on,
        st.gpa AS student_gpa
    FROM recent s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
course_id_stats AS (
    -- Avg course_credits per course_id
    SELECT course_id, AVG(course_credits) AS avg_course_credits
    FROM joined
    GROUP BY course_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_course_credits,
        ROUND(s.course_credits - g.avg_course_credits, 2) AS course_credits_minus_avg
    FROM joined s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.course_credits > g.avg_course_credits
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM vs_group s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND HOUR(e.enrolled_at) BETWEEN 8 AND 12
              AND e.score >= 34.2
        )
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', student_enrolled_on) AS student_enrolled_on_month,
        course_credits,
        course_id,
        ROUND(SUM(avg_course_credits) / NULLIF(SUM(course_credits_minus_avg), 0), 3) AS avg_course_credits_to_course_credits_minus_avg_r,
        SUM(course_credits_minus_avg) FILTER (WHERE UPPER(TRIM(course_code)) LIKE 'C%') AS course_credits_minus_avg_upper_trim_course_code_,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list
    FROM qualified
    GROUP BY DATE_TRUNC('MONTH', student_enrolled_on), course_credits, course_id
    HAVING MAX(course_credits) > 2
),
qualified_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE course_credits_minus_avg_upper_trim_course_code_ > (SELECT AVG(course_credits_minus_avg_upper_trim_course_code_) FROM per_group)
)
-- Native SQL UNPIVOT (wide to long)
SELECT student_enrolled_on_month, course_credits, metric, value
FROM (
    SELECT student_enrolled_on_month, course_credits, avg_course_credits_to_course_credits_minus_avg_r, course_credits_minus_avg_upper_trim_course_code_
    FROM qualified_2
) src
UNPIVOT (
    value FOR metric IN (avg_course_credits_to_course_credits_minus_avg_r AS m_avg_course_credits_to_course_credits_minus_avg, course_credits_minus_avg_upper_trim_course_code_ AS m_course_credits_minus_avg_upper_trim_course_cod)
) unp
ORDER BY student_enrolled_on_month, course_credits, metric
