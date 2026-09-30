WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter,
        YEAR(enrolled_at) AS enrolled_at_year,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref
    FROM enrollments
    WHERE term <> '2025T1'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY score ASC, id), 3) AS score_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY score ASC), 3) AS score_cume_dist_2,
        MIN(enrolled_at_year) OVER (PARTITION BY course_id) AS course_id_min_enrolled_at_year
    FROM prepared
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        score,
        enrolled_at_quarter,
        term_ref,
        score_cume_dist,
        score_cume_dist_2,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        COALESCE(score_cume_dist_2, 0.0) AS score_cume_dist_2_filled,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM with_windows
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.score,
        s.enrolled_at_quarter,
        s.term_ref,
        s.days_since_enrolled_at,
        s.term_slug,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.term AS enrollment_term
    FROM enriched s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
),
enriched_enrollment AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.score,
        s.enrolled_at_quarter,
        s.term_ref,
        s.days_since_enrolled_at,
        s.term_slug,
        s.enrollment_id,
        s.enrollment_term,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        st.cohort AS student_cohort,
        st.major AS student_major
    FROM line_level s
    JOIN enrollments e ON e.id = s.enrollment_id AND e.enrolled_at < TIMESTAMP '2025-03-02 09:00:00'
    LEFT JOIN students st ON st.id = e.student_id
)
SELECT
    id,
    student_id,
    enrolled_at_quarter,
    term_ref,
    days_since_enrolled_at,
    term_slug,
    enrollment_id,
    enrollment_term,
    enrollment_student_id,
    enrollment_grade_status,
    student_cohort,
    student_major,
    TO_DATE(enrolled_at_quarter) AS enrolled_at_quarter_date,
    ROUND(days_since_enrolled_at / NULLIF(score, 0), 3) AS days_since_enrolled_at_per_score
FROM enriched_enrollment
WHERE days_since_enrolled_at >= 245
ORDER BY id, days_since_enrolled_at
