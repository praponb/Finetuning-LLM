WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('|', term, grade_status) AS term_grade_status_label,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE grade_status = 'FAIL'
),
unique_rows AS (
    -- Keep the latest row per student_id, grade_status
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_grade_status_label,
        term_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at DESC, id DESC) AS recency_rank
        FROM prepared_enrollments
    ) ranked
    WHERE recency_rank = 1
),
latest_per_key AS (
    -- Keep the earliest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_grade_status_label,
        term_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY score, id) AS rn
        FROM unique_rows
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Add calculated columns
    SELECT *, ROUND(score * 1.5, 2) AS score_adj
    FROM latest_per_key
)
-- Subtotals with CUBE
SELECT
    student_id,
    course_id,
    GROUPING(student_id) AS is_total_student_id,
    MIN(enrolled_at) AS first_enrolled_at,
    ROUND(SUM(score_adj) / NULLIF(SUM(score), 0), 3) AS score_adj_to_score_ratio,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
FROM scored
GROUP BY CUBE(student_id, course_id)
