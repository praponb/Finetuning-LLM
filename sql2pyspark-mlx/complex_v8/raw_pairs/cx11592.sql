WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref
    FROM enrollments
),
deduped AS (
    -- Keep the latest row per grade_status, term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status, term ORDER BY enrolled_at DESC, id DESC) AS row_num
        FROM recent_enrollments
    ) ranked
    WHERE row_num = 1
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        c.dept AS course_dept,
        st.student_name,
        st.is_international AS student_is_international
    FROM deduped s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
course_dept_stats AS (
    -- Median score per course_dept
    SELECT course_dept, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM joined
    GROUP BY course_dept
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM joined s
    JOIN course_dept_stats g ON g.course_dept = s.course_dept
    WHERE s.score >= g.median_score
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    term_ref,
    student_name,
    student_is_international,
    median_score,
    score_minus_median,
    LENGTH(TRIM(course_dept)) AS course_dept_len
FROM compared
WHERE score_minus_median < 80.0
