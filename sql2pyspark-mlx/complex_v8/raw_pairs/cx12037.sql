WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
),
latest AS (
    -- Keep the earliest row per term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term ORDER BY score, id) AS rn
        FROM scoped
    ) ranked
    WHERE rn = 1
)
SELECT
    course_id,
    grade_status,
    MAX(enrolled_at) AS last_enrolled_at,
    SUM(score) FILTER (WHERE grade_status = 'INCOMPLETE') AS score_grade_status_incomplete
FROM latest
GROUP BY CUBE(course_id, grade_status)
ORDER BY course_id NULLS LAST, grade_status NULLS LAST
