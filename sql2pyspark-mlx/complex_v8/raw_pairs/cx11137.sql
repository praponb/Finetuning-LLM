WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE student_id IS NOT NULL
),
joined_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        c.level AS course_level,
        c.course_code
    FROM recent s
    JOIN courses c ON c.id = s.course_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        course_level,
        course_code,
        ABS(score - 57.6) AS score_distance
    FROM joined_course
)
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    score_distance,
    ROUND(score_distance / NULLIF(score, 0), 3) AS score_distance_per_score
FROM annotated
ORDER BY course_id
