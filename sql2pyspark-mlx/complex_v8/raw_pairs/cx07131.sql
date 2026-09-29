WITH base_enrollments AS (
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
joined_student AS (
    -- Join students
    SELECT s.*, st.cohort AS student_cohort
    FROM base_enrollments s
    JOIN students st ON st.id = s.student_id AND LOWER(st.student_name) LIKE '%sc%'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        SUM(score) OVER (PARTITION BY student_id, term ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        ROUND(AVG(score) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        ROUND(AVG(score) OVER (PARTITION BY term), 2) AS term_avg_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY term), 0), 4) AS score_share_of_term,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        LEAD(score, 1, 0) OVER w_grade_status_enrolled_at_to AS next_score,
        SUM(score) OVER (PARTITION BY term) AS term_sum_score,
        DENSE_RANK() OVER (PARTITION BY grade_status, TO_DATE(enrolled_at) ORDER BY enrolled_at DESC, id) AS enrolled_at_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY course_id)) AS distinct_grade_status_in_course_id,
        ROUND(CUME_DIST() OVER w_all_by_enrolled_at_ties, 3) AS enrolled_at_cume_dist,
        NTILE(3) OVER (PARTITION BY grade_status, TO_DATE(enrolled_at) ORDER BY enrolled_at DESC, id) AS enrolled_at_tercile
    FROM joined_student
    WINDOW w_term AS (PARTITION BY term), w_grade_status_enrolled_at_to AS (PARTITION BY grade_status, TO_DATE(enrolled_at) ORDER BY enrolled_at DESC, id), w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        student_cohort,
        rows_in_term,
        running_score,
        score_avg_7,
        term_avg_score,
        score_share_of_term,
        rows_in_course_id,
        next_score,
        term_sum_score,
        enrolled_at_percent_rank,
        first_known_enrolled_at,
        distinct_grade_status_in_course_id,
        enrolled_at_cume_dist,
        enrolled_at_tercile
    FROM windowed
    WHERE enrolled_at_dense_rank = 1
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    score,
    grade_status,
    enrolled_at,
    rows_in_term,
    score_avg_7,
    term_avg_score,
    score_share_of_term,
    rows_in_course_id,
    next_score,
    term_sum_score,
    enrolled_at_percent_rank,
    first_known_enrolled_at,
    distinct_grade_status_in_course_id,
    enrolled_at_cume_dist,
    enrolled_at_tercile
FROM top_per_group
