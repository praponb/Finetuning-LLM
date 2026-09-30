WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE course_id IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        NTH_VALUE(score, 2) OVER (ORDER BY score DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        LEAD(score, 2) OVER (ORDER BY score DESC NULLS FIRST, id) AS next2_score
    FROM filtered
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.second_score,
        s.next2_score,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status
    FROM sequenced s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        second_score,
        next2_score,
        enrollment_id,
        enrollment_score,
        enrollment_grade_status,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM detail_rows
    WHERE enrollment_score <= 71.0
),
screened AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.is_international = FALSE
        )
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    next2_score,
    enrollment_id,
    grade_status_slug
FROM screened
