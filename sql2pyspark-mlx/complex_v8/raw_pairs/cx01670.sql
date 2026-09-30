WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT *
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-03-02 09:00:00'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(enrolled_at) OVER w_all_by_enrolled_at AS prev_enrolled_at,
        ROUND(AVG(score) OVER w_enrolled_at_month_course_id, 2) AS enrolled_at_month_course_id_avg_score,
        RANK() OVER (PARTITION BY grade_status ORDER BY score DESC, id) AS score_rank,
        ROW_NUMBER() OVER w_all_by_enrolled_at AS enrolled_at_seq
    FROM scoped_enrollments
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at, id), w_enrolled_at_month_course_id AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), course_id)
),
joined AS (
    -- Attach course attributes
    SELECT s.*, c.level AS course_level
    FROM windowed s
    JOIN courses c ON c.id = s.course_id
)
SELECT
    student_id,
    COALESCE(course_level, 'ALL') AS course_level,
    GROUPING(student_id) AS is_total_student_id,
    COUNT(*) AS n_rows,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
    MAX(enrolled_at_seq) AS max_enrolled_at_seq
FROM joined
GROUP BY ROLLUP(student_id, course_level)
ORDER BY student_id NULLS LAST, course_level NULLS LAST
