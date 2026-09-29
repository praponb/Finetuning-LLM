WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref_2,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE grade_status IS NOT NULL
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count_2,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS min_enrollment_score
    FROM cleaned s
),
latest_per_key AS (
    -- Keep the earliest row per course_id, term
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        grade_status_ref,
        grade_status_ref_2,
        score_filled,
        enrollment_count,
        enrollment_count_2,
        min_enrollment_score
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id, term ORDER BY score_filled, id) AS row_num
        FROM summarized_rows
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    grade_status_ref,
    grade_status_ref_2,
    enrollment_count,
    enrollment_count_2,
    min_enrollment_score,
    enrolled_at + INTERVAL 7 DAYS AS enrolled_at_plus_7d,
    CASE WHEN grade_status IN ('FAIL', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'PASS' THEN 'group_b' ELSE 'other' END AS grade_status_group
FROM latest_per_key
WHERE score_filled BETWEEN 20.0 AND 65.0
ORDER BY course_id DESC NULLS FIRST
