WITH prepared_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        level,
        CONCAT_WS(': ', level, course_code) AS level_course_code_label,
        LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug
    FROM courses
    WHERE (dept = 'Biz' OR LOWER(course_code) LIKE '%ar%')
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        s.dept,
        s.level_course_code_label,
        s.dept_slug,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score
    FROM prepared_courses s
    JOIN enrollments e ON e.course_id = s.id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_code,
        level_course_code_label,
        enrollment_id,
        enrollment_term,
        ROUND(CUME_DIST() OVER (PARTITION BY dept ORDER BY enrollment_score DESC NULLS FIRST, id, enrollment_id), 3) AS enrollment_score_cume_dist,
        ROUND(enrollment_score / NULLIF(SUM(enrollment_score) OVER w_enrollment_term, 0), 4) AS enrollment_score_share_of_enrollment_term
    FROM exploded_detail
    WINDOW w_dept_by_enrollment_score AS (PARTITION BY dept ORDER BY enrollment_score DESC NULLS FIRST, id, enrollment_id), w_enrollment_term AS (PARTITION BY enrollment_term)
)
SELECT
    id,
    course_code,
    level_course_code_label,
    enrollment_id,
    enrollment_term,
    COALESCE(enrollment_term, 'UNKNOWN') AS enrollment_term_or_unknown
FROM with_trends
WHERE enrollment_id >= 60
ORDER BY enrollment_id
