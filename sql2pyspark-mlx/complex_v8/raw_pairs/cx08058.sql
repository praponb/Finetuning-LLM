WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj,
        CASE WHEN term IN ('2024T2', '2024T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY term, term_group ORDER BY enrolled_at, id) AS enrolled_at_tercile,
        FIRST_VALUE(score_adj) OVER (PARTITION BY grade_status ORDER BY score_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score_adj,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_student_id_by_enrolled_at)) / 60, 1) AS minutes_since_prev_enrolled_at,
        RANK() OVER (PARTITION BY grade_status ORDER BY score_adj DESC) AS score_adj_rank,
        MIN(score) OVER (PARTITION BY course_id) AS course_id_min_score,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY grade_status ORDER BY score_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at
    FROM valid_enrollments
    WINDOW w_student_id_by_enrolled_at AS (PARTITION BY student_id ORDER BY enrolled_at DESC, id)
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM with_windows s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT score_adj_rank, grade_status, term_group, first_score_adj
    FROM summarized_rows
) src
PIVOT (
    MAX(first_score_adj)
    FOR term_group IN ('other', 'group_b', 'group_a')
)
ORDER BY score_adj_rank, grade_status
