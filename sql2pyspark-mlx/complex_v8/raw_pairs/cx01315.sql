WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        CASE WHEN grade_status IN ('PASS', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE score >= 10.1
      AND course_id IS NOT NULL
),
latest_per_key AS (
    -- Keep the latest row per grade_status_group, term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_slug,
        grade_status_group,
        score_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status_group, term ORDER BY score_bucket DESC, id DESC) AS row_num
        FROM recent_enrollments
    ) ranked
    WHERE row_num = 1
)
SELECT *
FROM (
    SELECT grade_status_group, grade_status, score_bucket
    FROM latest_per_key
) src
PIVOT (
    SUM(score_bucket)
    FOR grade_status IN ('WITHDRAWN', 'FAIL')
)
