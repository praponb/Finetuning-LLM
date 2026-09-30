WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day,
        CASE WHEN term IN ('2024T1', '2025T2') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM enrollments
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM base_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
        )
      AND MONTH(s.enrolled_at_day) IN (2, 3)
),
per_group AS (
    -- Summarise per group
    SELECT
        term_group,
        SUM(CASE WHEN grade_status IS NOT NULL THEN score ELSE 0 END) AS score_grade_status_is_not_null,
        SUM(score) FILTER (WHERE grade_status IN ('INCOMPLETE', 'PASS')) AS score_grade_status_in_incomplete_p,
        COUNT(*) AS cnt
    FROM qualified
    GROUP BY term_group
    HAVING MAX(score) > 13.9
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN cnt >= 10 THEN 'HIGH' WHEN cnt >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS cnt_tier
    FROM per_group
)
SELECT *
FROM (
    SELECT cnt_tier, term_group, score_grade_status_is_not_null
    FROM labeled
) src
PIVOT (
    MAX(score_grade_status_is_not_null)
    FOR term_group IN ('other', 'group_b')
)
