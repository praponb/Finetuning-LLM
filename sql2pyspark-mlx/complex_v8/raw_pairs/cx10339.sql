WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(score, 0.0) AS score_filled,
        ABS(score - 32.9) AS score_distance
    FROM base_enrollments
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS max_enrollment_score
    FROM annotated s
)
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    student_id,
    GROUPING(grade_status) AS is_total_grade_status,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list
FROM with_counts
GROUP BY ROLLUP(grade_status, student_id)
ORDER BY grade_status NULLS LAST, student_id NULLS LAST
