WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE course_id <= 7
),
deduped AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        grade_status_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY score DESC, id DESC) AS row_num
        FROM recent
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with CUBE
SELECT
    student_id,
    term,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
    SUM(CASE WHEN grade_status = 'WITHDRAWN' THEN 1 ELSE 0 END) AS n_grade_status_withdrawn
FROM deduped
GROUP BY CUBE(student_id, term)
ORDER BY student_id NULLS LAST, term NULLS LAST
