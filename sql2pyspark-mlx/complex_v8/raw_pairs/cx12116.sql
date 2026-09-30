WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 39.4) AS score_distance
    FROM enrollments
    WHERE course_id IS NOT NULL
),
enriched AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        c.course_code
    FROM recent_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix
    FROM enriched
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        COUNT(*) AS cnt,
        COUNT(DISTINCT term) AS distinct_terms,
        COUNT(DISTINCT student_id) AS distinct_student_ids
    FROM derived
    GROUP BY DATE_TRUNC('MONTH', enrolled_at)
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_student_ids,
    distinct_terms,
    cnt,
    GROUPING(distinct_student_ids) AS is_total_distinct_student_ids,
    MAX(enrolled_at_month) AS last_enrolled_at_month
FROM summary
GROUP BY GROUPING SETS ((distinct_student_ids, distinct_terms, cnt), (distinct_student_ids), ())
ORDER BY distinct_student_ids NULLS LAST, distinct_terms NULLS LAST, cnt NULLS LAST
