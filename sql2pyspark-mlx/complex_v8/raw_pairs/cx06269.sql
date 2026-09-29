WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CASE WHEN grade_status IN ('INCOMPLETE', 'FAIL') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM enrollments
    WHERE student_id >= 11
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        grade_status_group,
        enrolled_at_week,
        YEAR(enrolled_at_week) AS enrolled_at_week_year
    FROM scoped
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        INITCAP(TRIM(grade_status_group)) AS grade_status_group_clean
    FROM derived
    WHERE course_id IS NOT NULL
),
latest_per_key AS (
    -- Keep the latest row per term, grade_status
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        grade_status_group,
        enrolled_at_week,
        enrolled_at_week_year,
        grade_status_slug,
        grade_status_group_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term, grade_status ORDER BY enrolled_at_week DESC, id DESC) AS recency_rank
        FROM labeled
    ) ranked
    WHERE recency_rank = 1
)
SELECT *
FROM (
    SELECT grade_status_group, term, grade_status, enrolled_at_week_year
    FROM latest_per_key
) src
PIVOT (
    COUNT(enrolled_at_week_year)
    FOR grade_status IN ('FAIL', 'INCOMPLETE', 'PASS')
)
ORDER BY grade_status_group, term
