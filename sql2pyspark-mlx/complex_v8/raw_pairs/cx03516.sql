WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
    WHERE student_id IS NOT NULL
),
term_stats AS (
    -- Median score per term
    SELECT term, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM filtered_enrollments
    GROUP BY term
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM filtered_enrollments s
    JOIN term_stats g ON g.term = s.term
    WHERE s.score >= g.median_score
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
        s.enrolled_at,
        s.median_score,
        s.score_minus_median,
        c.level AS course_level,
        c.credits AS course_credits,
        st.major AS student_major,
        st.student_name
    FROM outliers s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
unique_rows AS (
    -- Keep the latest row per course_level, student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        median_score,
        score_minus_median,
        course_level,
        course_credits,
        student_major,
        student_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_level, student_id ORDER BY course_credits DESC, id DESC) AS recency_rank
        FROM with_details
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    grade_status,
    course_level,
    course_credits,
    student_major,
    student_name,
    CONCAT(student_major, '#', CAST(student_id AS STRING)) AS student_major_ref,
    FLOOR(score / 25) * 25 AS score_bucket
FROM unique_rows
ORDER BY student_major_ref DESC NULLS FIRST
