WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND enrolled_at < TIMESTAMP '2025-02-18 10:00:00'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(term_len / 2) * 2 AS term_len_bucket,
        UPPER(TRIM(grade_status)) AS grade_status_clean,
        MONTH(enrolled_at) AS enrolled_at_month
    FROM scoped_enrollments
    WHERE course_id <= 7
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score >= 11.3
        )
),
combined_details AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.term_len_bucket,
        s.enrolled_at_month,
        c.credits AS course_credits,
        c.dept AS course_dept,
        c.course_code,
        st.enrolled_on AS student_enrolled_on
    FROM qualified s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', student_enrolled_on) AS student_enrolled_on_month,
        SUM(CASE WHEN course_dept IN ('Biz', 'Math', 'Art') THEN 1 ELSE 0 END) AS n_course_dept_in_biz_math_art,
        ROUND(AVG(course_credits), 2) AS avg_course_credits,
        MIN(score) AS min_score,
        SUM(score) AS total_score,
        PERCENTILE_APPROX(term_len_bucket, 0.5) AS median_term_len_bucket,
        SUM(score) FILTER (WHERE term NOT IN ('2025T2', '2024T1')) AS score_term_not_in_2025t2_2024t1
    FROM combined_details
    GROUP BY 1
),
tagged AS (
    -- Label and score rows
    SELECT *, FLOOR(min_score / 25) * 25 AS min_score_bucket
    FROM summary
),
labeled AS (
    -- Label and score rows
    SELECT
        student_enrolled_on_month,
        avg_course_credits,
        min_score,
        total_score,
        median_term_len_bucket,
        min_score_bucket,
        ROUND(min_score * 1.1, 2) AS min_score_adj,
        total_score + avg_course_credits AS total_score_plus_avg_course_credits,
        GREATEST(median_term_len_bucket, avg_course_credits) AS greatest_median_term_len_bucket_avg_course_credi
    FROM tagged
)
-- Final projection
SELECT
    student_enrolled_on_month,
    avg_course_credits,
    total_score,
    median_term_len_bucket,
    min_score_adj,
    total_score_plus_avg_course_credits,
    greatest_median_term_len_bucket_avg_course_credi,
    ABS(min_score_adj - 45.0) AS min_score_adj_distance
FROM labeled
WHERE min_score_bucket > 15
ORDER BY median_term_len_bucket
