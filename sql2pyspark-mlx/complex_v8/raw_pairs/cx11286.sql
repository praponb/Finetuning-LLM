WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE term NOT IN ('2025T1', '2025T2')
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score > 38.1
              AND e.course_id IS NOT NULL
        )
      AND s.grade_status NOT IN ('PASS', 'INCOMPLETE')
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM without_matches
    WHERE student_id >= 14
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT course_id, MIN(enrolled_at) AS first_enrolled_at, COUNT(*) AS cnt
    FROM enriched
    WHERE score < 83.1
    GROUP BY course_id
    HAVING COUNT(*) >= 2
)
SELECT
    course_id,
    first_enrolled_at,
    QUARTER(first_enrolled_at) AS first_enrolled_at_quarter,
    DATE_TRUNC('QUARTER', first_enrolled_at) AS first_enrolled_at_quarter_2
FROM rollup_base
ORDER BY first_enrolled_at_quarter, first_enrolled_at DESC
