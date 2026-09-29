WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('PASS', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM enrollments
    WHERE student_id <= 7
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('|', grade_status_group, grade_status) AS grade_status_group_grade_status_label,
        MONTH(enrolled_at) AS enrolled_at_month,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM filtered
)
-- Subtotals with ROLLUP
SELECT
    enrolled_at_month,
    course_id,
    GROUPING_ID() AS gid,
    MAX(enrolled_at_month) AS max_enrolled_at_month,
    SUM(CASE WHEN grade_status NOT IN ('FAIL', 'WITHDRAWN') THEN 1 ELSE 0 END) AS n_grade_status_not_in_fail_wit
FROM tagged
GROUP BY ROLLUP(enrolled_at_month, course_id)
ORDER BY enrolled_at_month NULLS LAST, course_id NULLS LAST
