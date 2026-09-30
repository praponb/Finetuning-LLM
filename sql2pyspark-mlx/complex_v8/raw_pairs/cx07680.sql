WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CASE WHEN score >= 73.0 THEN 'HIGH' WHEN score >= 38.5 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE (student_id <= 18 OR HOUR(enrolled_at) BETWEEN 10 AND 14)
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.score_tier,
        s.score_filled,
        c.course_code,
        c.credits AS course_credits,
        c.dept AS course_dept,
        st.student_name,
        st.major AS student_major
    FROM recent s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, MIN(score) AS min_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score
    FROM with_parent_course s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
-- Subtotals with ROLLUP
SELECT
    course_dept,
    term,
    SUM(CASE WHEN term = '2024T1' THEN course_credits ELSE 0 END) AS course_credits_term_2024t1,
    COUNT(DISTINCT student_id) AS distinct_student_ids
FROM with_counts
GROUP BY ROLLUP(course_dept, term)
