WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE term IN ('2024T1', '2025T2')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_or_unknown,
        NTILE(5) OVER w_course_id_term_by_enrolled_a AS enrolled_at_quintile,
        MIN(days_since_enrolled_at) OVER w_term_or_unknown_course_id AS term_or_unknown_course_id_min_days_since_enrolle,
        MIN(score) OVER (PARTITION BY term_or_unknown, course_id) AS term_or_unknown_course_id_min_score,
        ROUND(days_since_enrolled_at - AVG(days_since_enrolled_at) OVER (PARTITION BY term_or_unknown, course_id), 2) AS days_since_enrolled_at_vs_term_or_unknown_course,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_id, term ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score
    FROM filtered
    WINDOW w_course_id_term_by_enrolled_a AS (PARTITION BY course_id, term ORDER BY enrolled_at ASC, id), w_term_or_unknown_course_id AS (PARTITION BY term_or_unknown, course_id)
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
      AND s.grade_status IS NOT NULL
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM without_matches s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score > 5.7
              AND e.term IS NOT NULL
        )
      AND s.course_id IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_at_quintile, grade_status, term_or_unknown_course_id_min_days_since_enrolle
    FROM qualified
) src
PIVOT (
    MAX(term_or_unknown_course_id_min_days_since_enrolle)
    FOR grade_status IN ('FAIL', 'WITHDRAWN')
)
ORDER BY enrolled_at_quintile
