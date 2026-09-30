WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix
    FROM enrollments
    WHERE (HOUR(enrolled_at) BETWEEN 9 AND 14 OR score < 87.8)
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM scoped_enrollments s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
            WHERE e.score >= 35.8
        )
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        course_id,
        grade_status,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        SUM(score) AS total_score,
        COUNT(*) AS cnt
    FROM qualified
    WHERE grade_status_prefix IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', enrolled_at), course_id, grade_status
)
SELECT
    enrolled_at_month,
    grade_status,
    grade_status_list,
    total_score,
    cnt,
    CASE WHEN grade_status IN ('PASS', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group
FROM totals
ORDER BY cnt, enrolled_at_month
