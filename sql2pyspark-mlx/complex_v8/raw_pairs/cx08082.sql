WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
    WHERE credits >= 2
      AND LOWER(course_code) LIKE '%ar%'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(credits, 0) AS credits_filled,
        ABS(credits - 3) AS credits_distance
    FROM cleaned
    WHERE credits > 2
),
enriched AS (
    -- Derive helper columns
    SELECT *, COALESCE(credits_filled, 0) AS credits_filled_filled
    FROM labeled
    WHERE dept IS NOT NULL
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        s.dept,
        s.level,
        s.credits_distance,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM enriched s
    LEFT JOIN enrollments e ON e.course_id = s.id
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM exploded_detail s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.student_id IS NOT NULL
        )
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        course_code,
        level,
        credits_distance,
        enrollment_id,
        enrollment_term,
        LOWER(REGEXP_REPLACE(TRIM(level), ' ', '_')) AS level_slug,
        COALESCE(credits_distance, 0) AS credits_distance_filled
    FROM without_matches
)
SELECT *
FROM (
    SELECT enrollment_term, enrollment_id, level, credits_distance_filled
    FROM scored
) src
PIVOT (
    COUNT(credits_distance_filled) AS count_v, MAX(credits_distance_filled) AS max_v
    FOR level IN ('intro', 'core', 'advanced')
)
ORDER BY enrollment_term, enrollment_id
