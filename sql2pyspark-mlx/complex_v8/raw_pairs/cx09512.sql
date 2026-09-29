WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 13:00:00'
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM prepared_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
      AND s.term = '2024T2'
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
        term_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(student_id) OVER w_term_course_id) AS distinct_student_id_in_term_course_id,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY term ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        DENSE_RANK() OVER w_term_by_score_ties AS score_dense_rank
    FROM latest
    WINDOW w_term_course_id AS (PARTITION BY term, course_id), w_term_by_score_ties AS (PARTITION BY term ORDER BY score)
)
SELECT
    student_id,
    distinct_student_id_in_term_course_id,
    MIN(second_enrolled_at) AS first_second_enrolled_at,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
    MAX(distinct_student_id_in_term_course_id) AS max_distinct_student_id_in_term_course_id
FROM ranked
GROUP BY GROUPING SETS ((student_id, distinct_student_id_in_term_course_id), (student_id), ())
