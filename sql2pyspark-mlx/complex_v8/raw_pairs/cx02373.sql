WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-02-06 10:00:00'
),
with_parent_course AS (
    -- Join courses
    SELECT s.*, c.credits AS course_credits, c.level AS course_level
    FROM recent s
    LEFT JOIN courses c ON c.id = s.course_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        grade_status_or_unknown,
        course_credits,
        LEAD(course_credits, 1, 0) OVER (ORDER BY enrolled_at, id) AS next_course_credits,
        MAX(score) OVER (PARTITION BY grade_status_or_unknown, grade_status ORDER BY score DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS score_max_3_prior,
        LAG(course_credits) OVER (PARTITION BY course_credits ORDER BY enrolled_at ASC, id) AS prev_course_credits,
        RANK() OVER (PARTITION BY term ORDER BY course_credits) AS course_credits_rank,
        RANK() OVER (ORDER BY enrolled_at) AS enrolled_at_rank,
        ROW_NUMBER() OVER (PARTITION BY course_credits ORDER BY enrolled_at ASC, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY enrolled_at, id) AS enrolled_at_seq,
        ROUND(AVG(course_credits) OVER (PARTITION BY course_id), 2) AS course_id_avg_course_credits,
        LAG(course_credits, 2) OVER w_all_by_score AS prev2_course_credits,
        ROUND(CUME_DIST() OVER (PARTITION BY course_credits ORDER BY enrolled_at ASC), 3) AS enrolled_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY course_credits, course_level ORDER BY course_credits DESC) AS course_credits_dense_rank,
        LEAD(course_credits, 2, 0) OVER (ORDER BY enrolled_at, id) AS next2_course_credits,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_credits, course_level ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROW_NUMBER() OVER w_all_by_score AS rn
    FROM with_parent_course
    WINDOW w_all_by_score AS (ORDER BY score ASC, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE enrolled_at_seq <= 5
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY next2_course_credits ORDER BY course_credits_rank ASC) AS course_credits_rank_dense_rank,
        COUNT(*) OVER w_student_id_seq_no AS rows_in_student_id_seq_no,
        enrolled_at_seq - LAG(enrolled_at_seq) OVER (ORDER BY next2_course_credits DESC, id) AS enrolled_at_seq_change,
        MAX(enrolled_at_cume_dist) OVER (ORDER BY rn DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS enrolled_at_cume_dist_max_3,
        MIN(course_id_avg_course_credits) OVER (PARTITION BY student_id, seq_no) AS student_id_seq_no_min_course_id_avg_course_credi,
        MAX(enrolled_at_cume_dist) OVER (PARTITION BY course_credits_dense_rank, grade_status) AS course_credits_dense_rank_grade_status_max_enrol
    FROM first_rows
    WINDOW w_student_id_seq_no AS (PARTITION BY student_id, seq_no)
),
per_group AS (
    -- Summarise per group
    SELECT
        grade_status,
        course_credits_rank,
        DATE_TRUNC('MONTH', first_known_enrolled_at) AS first_known_enrolled_at_month,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        SUM(course_credits_dense_rank) FILTER (WHERE grade_status IN ('PASS', 'FAIL', 'INCOMPLETE')) AS course_credits_dense_rank_grade_status_in_pass_f,
        SUM(course_credits_rank_dense_rank) AS total_course_credits_rank_dense_rank,
        COUNT(*) AS n_rows
    FROM sequenced
    GROUP BY grade_status, course_credits_rank, DATE_TRUNC('MONTH', first_known_enrolled_at)
)
-- Final projection
SELECT
    grade_status,
    course_credits_rank,
    first_known_enrolled_at_month,
    grade_status_list,
    course_credits_dense_rank_grade_status_in_pass_f,
    total_course_credits_rank_dense_rank,
    COALESCE(course_credits_rank, 0) AS course_credits_rank_filled,
    COALESCE(n_rows, 0) AS n_rows_filled
FROM per_group
ORDER BY course_credits_rank_filled, n_rows_filled DESC
