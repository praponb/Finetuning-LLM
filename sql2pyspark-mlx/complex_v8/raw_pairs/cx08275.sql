WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_all_by_score_ties AS score_rank,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at
    FROM filtered_enrollments
    WINDOW w_all_by_score_ties AS (ORDER BY score ASC)
)
SELECT
    student_id,
    course_id,
    term,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
    COUNT(DISTINCT course_id) AS distinct_course_ids,
    COUNT(DISTINCT term) AS distinct_terms
FROM windowed
GROUP BY ROLLUP(student_id, course_id, term)
