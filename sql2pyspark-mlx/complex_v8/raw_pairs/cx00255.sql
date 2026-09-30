WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE grade_status IS NOT NULL
      AND term NOT IN ('2024T1', '2025T2')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        s.grade_status_clean,
        c.dept AS course_dept
    FROM filtered s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept = 'Biz'
),
combined_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.grade_status_clean,
        s.course_dept,
        c.dept AS course_dept_2,
        c.course_code,
        c.credits AS course_credits
    FROM with_parent s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept <> 'Math'
),
tagged AS (
    -- Label and score rows
    SELECT *, COALESCE(course_dept, 'UNKNOWN') AS course_dept_or_unknown
    FROM combined_details_course
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at,
        grade_status_clean,
        course_dept,
        course_dept_2,
        course_code,
        course_credits,
        course_dept_or_unknown,
        course_credits - LAG(course_credits) OVER w_course_credits_by_course_cre AS course_credits_change,
        COUNT(*) OVER (PARTITION BY course_dept, student_id) AS rows_in_course_dept_student_id
    FROM tagged
    WINDOW w_course_credits_by_course_cre AS (PARTITION BY course_credits ORDER BY course_credits DESC, id)
),
latest_per_key AS (
    -- Keep the earliest row per course_dept_or_unknown
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at,
        grade_status_clean,
        course_dept,
        course_dept_2,
        course_code,
        course_credits,
        course_dept_or_unknown,
        course_credits_change,
        rows_in_course_dept_student_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_dept_or_unknown ORDER BY course_credits, id) AS recency_rank
        FROM ranked
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    grade_status_clean,
    course_dept,
    course_credits,
    course_dept_or_unknown,
    course_credits_change,
    rows_in_course_dept_student_id,
    LOWER(REGEXP_REPLACE(TRIM(course_dept_or_unknown), ' ', '_')) AS course_dept_or_unknown_slug
FROM latest_per_key
WHERE course_dept_or_unknown = 'UNKNOWN'
ORDER BY course_dept DESC, course_credits, id
LIMIT 5
