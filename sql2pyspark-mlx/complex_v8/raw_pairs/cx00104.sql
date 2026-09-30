WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label,
        CASE WHEN term IN ('2024T2', '2025T2') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE grade_status NOT IN ('FAIL', 'WITHDRAWN')
      AND enrolled_at >= TIMESTAMP '2025-02-06 11:00:00'
),
unique_rows AS (
    -- Keep the latest row per term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        grade_status_term_label,
        term_group,
        term_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term ORDER BY term_len DESC, id DESC) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with ROLLUP
SELECT
    student_id,
    course_id,
    COALESCE(term, 'ALL') AS term,
    GROUPING(student_id) AS is_total_student_id,
    ROUND(SUM(score), 2) AS total_score
FROM unique_rows
GROUP BY ROLLUP(student_id, course_id, term)
ORDER BY student_id NULLS LAST, course_id NULLS LAST, term NULLS LAST
