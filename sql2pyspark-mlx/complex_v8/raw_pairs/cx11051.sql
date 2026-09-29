WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref_2
    FROM enrollments
    WHERE course_id >= 7
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(enrolled_at_month, 'yyyy-MM') AS enrolled_at_month_ym,
        course_id,
        grade_status,
        ROUND(100 * COUNT(CASE WHEN grade_status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_grade_status_is_not_null,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        MAX(enrolled_at) AS last_enrolled_at,
        ROUND(SUM(score), 2) AS total_score
    FROM prepared
    GROUP BY DATE_FORMAT(enrolled_at_month, 'yyyy-MM'), course_id, grade_status
    HAVING COUNT(*) >= 2
),
flattened AS (
    -- One row per grade_status_list_item
    SELECT
        s.enrolled_at_month_ym,
        s.course_id,
        s.grade_status,
        s.pct_grade_status_is_not_null,
        s.row_count,
        s.last_enrolled_at,
        s.total_score,
        t.grade_status_list_item
    FROM summary s
    LATERAL VIEW OUTER EXPLODE(s.grade_status_list) t AS grade_status_list_item
)
SELECT enrolled_at_month_ym, course_id, field, amount
FROM (
    SELECT enrolled_at_month_ym, course_id, pct_grade_status_is_not_null, total_score
    FROM flattened
) src
UNPIVOT (
    amount FOR field IN (pct_grade_status_is_not_null, total_score)
) unp
