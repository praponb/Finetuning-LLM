WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(TRIM(term)) AS term_clean,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
),
without_matches AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM recent_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.gpa < 3.4
              AND st.major <> 'Biz'
        )
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at_date,
        days_since_enrolled_at,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year,
        UPPER(TRIM(term)) AS term_clean_2
    FROM without_matches
    WHERE enrolled_at_date IS NOT NULL
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        SUM(score) AS total_enrollment_score,
        MAX(score) AS max_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.total_enrollment_score,
        e.max_enrollment_score
    FROM tagged s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
-- Subtotals with ROLLUP
SELECT
    course_id,
    term,
    enrollment_count,
    GROUPING(course_id) AS is_total_course_id,
    PERCENTILE_APPROX(score, 0.5) AS median_score,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_stats
GROUP BY ROLLUP(course_id, term, enrollment_count)
ORDER BY course_id NULLS LAST, term NULLS LAST, enrollment_count NULLS LAST
