WITH scoped AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug
    FROM courses
    WHERE dept <> 'Bio'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(credits - 2) AS credits_distance,
        LENGTH(TRIM(level)) AS level_len,
        INITCAP(TRIM(level)) AS level_clean
    FROM scoped
    WHERE LOWER(course_code) LIKE '%a1%'
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.id) AS enrollment_count,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.id) AS last_enrollment_enrolled_at
    FROM labeled s
)
SELECT id, field, reading
FROM (
    SELECT id, level_len, credits_distance, enrollment_count
    FROM with_counts
) src
UNPIVOT (
    reading FOR field IN (level_len, credits_distance, enrollment_count)
) unp
