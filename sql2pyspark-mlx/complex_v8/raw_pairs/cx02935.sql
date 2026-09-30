WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM recent_enrollments
),
summary AS (
    -- Summarise per group
    SELECT
        term,
        student_id,
        SUM(score) AS total_score,
        ROUND(STDDEV(score), 2) AS stddev_score
    FROM tagged
    GROUP BY term, student_id
)
SELECT
    term,
    student_id,
    stddev_score,
    SUBSTRING(TRIM(term), 1, 4) AS term_prefix
FROM summary
