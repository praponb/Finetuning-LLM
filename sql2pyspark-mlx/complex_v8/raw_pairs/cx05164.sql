WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 2)
      AND grade_status IN ('FAIL', 'WITHDRAWN')
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        grade_status,
        SUM(CASE WHEN term IN ('2024T2', '2025T2', '2025T1') THEN 1 ELSE 0 END) AS n_term_in_2024t2_2025t2_2025t1,
        MAX(score) AS max_score
    FROM filtered_enrollments
    WHERE term IS NOT NULL
    GROUP BY 1, 2
),
combined_details_course AS (
    -- Join courses
    SELECT
        s.course_id,
        s.grade_status,
        s.n_term_in_2024t2_2025t2_2025t1,
        s.max_score,
        c.credits AS course_credits,
        c.level AS course_level,
        c.course_code
    FROM totals s
    LEFT JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(course_credits) OVER (PARTITION BY n_term_in_2024t2_2025t2_2025t1, grade_status) AS n_term_in_2024t2_2025t2_2025t1_grade_status_sum_,
        COUNT(*) OVER (ORDER BY max_score DESC, course_id, grade_status ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(max_score) IGNORE NULLS OVER (ORDER BY max_score DESC, course_id, grade_status ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_score,
        ROUND(course_credits - AVG(course_credits) OVER (PARTITION BY n_term_in_2024t2_2025t2_2025t1, grade_status), 2) AS course_credits_vs_n_term_in_2024t2_2025t2_2025t1
    FROM combined_details_course
),
combined_details_course_2 AS (
    -- Enrich with dimension data
    SELECT s.*, c.course_code AS course_course_code
    FROM metrics s
    LEFT JOIN courses c ON c.id = s.course_id AND c.level = 'advanced'
),
with_details AS (
    -- Attach course attributes
    SELECT
        s.*,
        c.credits AS course_credits_2,
        c.course_code AS course_course_code_2,
        c.level AS course_level_2
    FROM combined_details_course_2 s
    LEFT JOIN courses c ON c.id = s.course_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT course_level_2, course_id, grade_status, n_term_in_2024t2_2025t2_2025t1_grade_status_sum_
    FROM with_details
) src
PIVOT (
    MIN(n_term_in_2024t2_2025t2_2025t1_grade_status_sum_) AS min_v, COUNT(n_term_in_2024t2_2025t2_2025t1_grade_status_sum_) AS n
    FOR grade_status IN ('PASS', 'WITHDRAWN', 'INCOMPLETE', 'FAIL')
)
ORDER BY course_level_2, course_id
