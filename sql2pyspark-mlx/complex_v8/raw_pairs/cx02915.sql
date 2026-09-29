WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(': ', grade_status, term) AS grade_status_term_label,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref,
        ROUND(score * 1.1, 2) AS score_adj
    FROM enrollments
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_course_id_student_id_by_scor, 3) AS score_adj_cume_dist,
        COUNT(*) OVER (PARTITION BY YEAR(enrolled_at)) AS rows_in_enrolled_at_year
    FROM recent
    WINDOW w_course_id_student_id_by_scor AS (PARTITION BY course_id, student_id ORDER BY score_adj DESC, id)
),
unique_rows AS (
    -- Keep the latest row per course_id, student_id
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        grade_status_term_label,
        grade_status_ref,
        score_adj,
        score_adj_cume_dist,
        rows_in_enrolled_at_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id, student_id ORDER BY enrolled_at DESC, id DESC) AS recency_rank
        FROM with_trends
    ) ranked
    WHERE recency_rank = 1
),
unique_rows_2 AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        grade_status_term_label,
        grade_status_ref,
        score_adj,
        score_adj_cume_dist,
        rows_in_enrolled_at_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id DESC) AS recency_rank
        FROM unique_rows
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    student_id,
    course_id,
    enrolled_at,
    grade_status_term_label,
    grade_status_ref,
    score_adj_cume_dist,
    rows_in_enrolled_at_year
FROM unique_rows_2
ORDER BY grade_status_ref DESC, enrolled_at
