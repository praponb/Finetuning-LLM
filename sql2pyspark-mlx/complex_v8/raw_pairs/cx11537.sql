WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE term = '2025T1'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(grade_status) OVER w_student_id_course_id) AS distinct_grade_status_in_student_id_course_id,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status, course_id ORDER BY score, id), 3) AS score_cume_dist
    FROM cleaned
    WINDOW w_student_id_course_id AS (PARTITION BY student_id, course_id)
),
latest AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        grade_status_slug,
        distinct_grade_status_in_student_id_course_id,
        score_cume_dist
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY distinct_grade_status_in_student_id_course_id DESC, id DESC) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    student_id,
    score,
    enrolled_at,
    score_filled,
    grade_status_slug,
    distinct_grade_status_in_student_id_course_id,
    score_cume_dist
FROM latest
ORDER BY score DESC
