WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE student_id IS NOT NULL
),
with_parent_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at_date,
        c.level AS course_level,
        c.course_code,
        c.dept AS course_dept,
        st.is_international AS student_is_international
    FROM base s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
    WHERE c.level = 'core'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN course_level IN ('intro', 'advanced') THEN 'group_a' WHEN course_level = 'core' THEN 'group_b' ELSE 'other' END AS course_level_group,
        COALESCE(score, 0.0) AS score_filled,
        CASE WHEN score >= 57.6 THEN 'HIGH' WHEN score >= 11.3 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM with_parent_course
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(score_filled) IGNORE NULLS OVER (PARTITION BY student_id, course_dept ORDER BY score_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_filled,
        DENSE_RANK() OVER (PARTITION BY student_id, course_dept ORDER BY score_filled, id) AS score_filled_dense_rank,
        ROUND(score_filled - AVG(score_filled) OVER (PARTITION BY student_id), 2) AS score_filled_vs_student_id_avg
    FROM scored
)
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at_date,
    course_level,
    course_level_group,
    score_filled,
    score_tier,
    score_filled_dense_rank,
    CASE WHEN student_is_international THEN 1 ELSE 0 END AS student_is_international_flag
FROM windowed
WHERE course_id <= 5
ORDER BY grade_status, course_id DESC, id
LIMIT 10
