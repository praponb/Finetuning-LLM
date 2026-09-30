WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE grade_status IN ('FAIL', 'INCOMPLETE')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        term,
        enrolled_at,
        MIN(score) OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        SUM(score) OVER w_course_id AS course_id_sum_score
    FROM filtered
    WINDOW w_course_id AS (PARTITION BY course_id)
),
with_details AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.min_score_so_far,
        s.course_id_sum_score,
        c.dept AS course_dept
    FROM windowed s
    LEFT JOIN courses c ON c.id = s.course_id
),
unique_rows AS (
    -- Keep the latest row per course_dept, term
    SELECT
        id,
        course_id,
        term,
        enrolled_at,
        min_score_so_far,
        course_id_sum_score,
        course_dept
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_dept, term ORDER BY enrolled_at DESC, id DESC) AS rn
        FROM with_details
    ) ranked
    WHERE rn = 1
)
SELECT *
FROM (
    SELECT term, course_dept, course_id_sum_score
    FROM unique_rows
) src
PIVOT (
    AVG(course_id_sum_score)
    FOR course_dept IN ('Art', 'Bio', 'Math', 'Biz')
)
