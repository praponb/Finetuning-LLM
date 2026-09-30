WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref,
        ROUND(score * 0.25, 2) AS score_adj,
        UPPER(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND score <= 77.0
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.term_ref,
        s.term_clean,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term
    FROM recent s
    JOIN enrollments e ON e.student_id = s.student_id
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM line_level s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score >= 36.6
              AND e.student_id >= 7
        )
),
stats AS (
    -- Group-level totals
    SELECT
        term,
        MIN(enrolled_at) AS first_enrolled_at,
        COUNT(*) FILTER (WHERE enrollment_term IS NOT NULL) AS n_enrollment_term_is_not_null,
        PERCENTILE_APPROX(enrollment_score, 0.5) AS median_enrollment_score
    FROM without_matches
    WHERE LOWER(term_clean) LIKE '%20%'
    GROUP BY 1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        term,
        first_enrolled_at,
        n_enrollment_term_is_not_null,
        ROW_NUMBER() OVER (PARTITION BY n_enrollment_term_is_not_null, term ORDER BY median_enrollment_score ASC, term) AS seq_no,
        NTILE(10) OVER (PARTITION BY term ORDER BY median_enrollment_score DESC, term) AS median_enrollment_score_decile
    FROM stats
),
annotated AS (
    -- Label and score rows
    SELECT *, TO_DATE(first_enrolled_at) AS first_enrolled_at_date
    FROM metrics
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM annotated
    WHERE median_enrollment_score_decile < (SELECT MAX(median_enrollment_score_decile) FROM annotated)
)
-- Final projection
SELECT
    term,
    seq_no,
    median_enrollment_score_decile,
    first_enrolled_at_date,
    LENGTH(TRIM(term)) AS term_len
FROM qualified
ORDER BY first_enrolled_at_date DESC
