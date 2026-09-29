WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT *
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 8 AND 11
      AND course_id IS NOT NULL
),
rollup_base AS (
    -- Group-level totals
    SELECT
        student_id,
        course_id,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        SUM(CASE WHEN term = '2025T1' THEN 1 ELSE 0 END) AS n_term_2025t1,
        SUM(CASE WHEN term IN ('2024T2', '2025T1', '2024T1') THEN 1 ELSE 0 END) AS n_term_in_2024t2_2025t1_2024t1,
        MAX(enrolled_at) AS last_enrolled_at,
        MIN(score) AS min_score
    FROM recent_enrollments
    GROUP BY student_id, course_id, DATE_TRUNC('MONTH', enrolled_at)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM rollup_base
    WHERE cnt > (SELECT MIN(cnt) FROM rollup_base)
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term = '2025T2'
              AND e.enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
        )
)
SELECT
    student_id,
    enrolled_at_month,
    cnt,
    term_list,
    n_term_2025t1,
    n_term_in_2024t2_2025t1_2024t1,
    last_enrolled_at,
    DATEDIFF(DATE '2025-12-31', enrolled_at_month) AS days_since_enrolled_at_month
FROM filtered
ORDER BY cnt DESC, n_term_in_2024t2_2025t1_2024t1 DESC
