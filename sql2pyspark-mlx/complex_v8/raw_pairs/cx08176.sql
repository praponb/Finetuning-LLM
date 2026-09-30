WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        UPPER(TRIM(term)) AS term_clean,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (2, 3)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        term_clean,
        enrolled_at_date,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM cleaned
),
course_id_stats AS (
    -- Median score per course_id
    SELECT course_id, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM annotated
    GROUP BY course_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM annotated s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score >= g.median_score
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM outliers s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
            WHERE e.term = '2024T2'
              AND e.score <= 90.9
        )
      AND s.enrolled_at < TIMESTAMP '2025-03-02 09:00:00'
),
course_id_stats_2 AS (
    -- Median score_bucket per course_id
    SELECT course_id, PERCENTILE_APPROX(score_bucket, 0.5) AS median_score_bucket
    FROM filtered
    GROUP BY course_id
),
outliers_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score_bucket,
        ROUND(s.score_bucket - g.median_score_bucket, 2) AS score_bucket_minus_median
    FROM filtered s
    JOIN course_id_stats_2 g ON g.course_id = s.course_id
    WHERE s.score_bucket >= g.median_score_bucket
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.term_clean,
        s.enrolled_at_date,
        s.score_bucket,
        s.score_minus_median,
        st.major AS student_major,
        c.credits AS course_credits,
        c.course_code
    FROM outliers_2 s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    term_clean,
    enrolled_at_date,
    score_bucket,
    score_minus_median,
    student_major,
    course_credits,
    course_code,
    FLOOR(score_bucket / 25) * 25 AS score_bucket_bucket,
    enrolled_at + INTERVAL 15 MINUTES AS enrolled_at_plus_15m
FROM combined_details
ORDER BY course_code NULLS LAST
