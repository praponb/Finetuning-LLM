WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at,
        UPPER(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE term NOT IN ('2025T1', '2024T2')
      AND student_id IS NOT NULL
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, COUNT(*) AS enrollment_count, COUNT(*) AS enrollment_count_2
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2
    FROM base s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM profiled s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
        )
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(days_since_enrolled_at, score) AS least_days_since_enrolled_at_score,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        score - enrollment_count_2 AS score_minus_enrollment_count_2
    FROM without_matches
),
combined_details AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.days_since_enrolled_at,
        s.term_clean,
        s.enrollment_count_2,
        s.least_days_since_enrolled_at_score,
        s.term_or_unknown,
        s.score_minus_enrollment_count_2,
        c.course_code,
        c.credits AS course_credits,
        st.enrolled_on AS student_enrolled_on,
        st.student_name
    FROM enriched s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    term_clean,
    enrollment_count_2,
    score_minus_enrollment_count_2,
    course_credits,
    student_enrolled_on,
    student_name,
    CASE WHEN term IN ('2025T2', '2024T2') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group,
    course_credits + days_since_enrolled_at AS course_credits_plus_days_since_enrolled_at
FROM combined_details
ORDER BY course_credits_plus_days_since_enrolled_at
