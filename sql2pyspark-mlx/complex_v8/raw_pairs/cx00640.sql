WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-18 08:00:00'
      AND score <= 78.8
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, c.credits AS course_credits, c.course_code
    FROM base_enrollments s
    JOIN courses c ON c.id = s.course_id
),
combined_details AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.days_since_enrolled_at,
        s.term_slug,
        s.enrolled_at_plus_30m,
        s.course_code,
        c.credits AS course_credits_2,
        st.cohort AS student_cohort
    FROM enriched s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    term_slug,
    enrolled_at_plus_30m,
    course_credits_2,
    DATE_FORMAT(enrolled_at_plus_30m, 'yyyy-MM-dd HH:00') AS enrolled_at_plus_30m_label,
    QUARTER(enrolled_at_plus_30m) AS enrolled_at_plus_30m_quarter
FROM combined_details
ORDER BY score DESC NULLS FIRST
