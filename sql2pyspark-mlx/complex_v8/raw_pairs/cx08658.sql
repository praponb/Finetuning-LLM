WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE student_id IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        grade_status_term_label,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        MIN(score) OVER (PARTITION BY grade_status) AS grade_status_min_score,
        DENSE_RANK() OVER w_course_id_student_id_by_scor AS score_dense_rank,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_id, student_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        LEAD(score) OVER (PARTITION BY course_id, student_id ORDER BY score, id) AS next_score,
        NTILE(5) OVER w_course_id_student_id_by_scor_3 AS score_quintile
    FROM base
    WINDOW w_course_id_student_id_by_scor AS (PARTITION BY course_id, student_id ORDER BY score), w_course_id_student_id_by_scor_3 AS (PARTITION BY course_id, student_id ORDER BY score, id)
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
        )
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    score_dense_rank,
    GROUPING_ID() AS gid,
    COUNT(*) AS n_rows
FROM filtered
GROUP BY GROUPING SETS ((grade_status, score_dense_rank), (grade_status), ())
ORDER BY grade_status NULLS LAST, score_dense_rank NULLS LAST
