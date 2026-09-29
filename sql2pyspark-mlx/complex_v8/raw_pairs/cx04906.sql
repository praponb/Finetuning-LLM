WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE grade_status IS NOT NULL
),
joined_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.score_bucket,
        c.course_code
    FROM scoped_enrollments s
    JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY grade_status, course_id ORDER BY score_bucket ASC, id) AS score_bucket_tercile,
        NTILE(4) OVER (PARTITION BY student_id, term ORDER BY enrolled_at, id) AS enrolled_at_quartile,
        MAX(score_bucket) OVER (PARTITION BY course_id, grade_status) AS course_id_grade_status_max_score_bucket,
        ROW_NUMBER() OVER w_student_id_course_id_by_scor AS seq_no,
        SIZE(COLLECT_SET(student_id) OVER w_course_id_grade_status) AS distinct_student_id_in_course_id_grade_status,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, term ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank,
        ROUND(PERCENT_RANK() OVER w_student_id_term_by_enrolled, 3) AS enrolled_at_percent_rank_2,
        COUNT(*) OVER (PARTITION BY course_id, grade_status) AS rows_in_course_id_grade_status,
        LAG(enrolled_at, 2) OVER (PARTITION BY student_id, term ORDER BY enrolled_at, id) AS prev2_enrolled_at,
        LAG(enrolled_at) OVER w_student_id_course_id_by_scor AS prev_enrolled_at,
        ROW_NUMBER() OVER (PARTITION BY grade_status, course_id ORDER BY score_bucket ASC, id) AS rn
    FROM joined_course
    WINDOW w_student_id_course_id_by_scor AS (PARTITION BY student_id, course_id ORDER BY score_bucket DESC, id), w_course_id_grade_status AS (PARTITION BY course_id, grade_status), w_student_id_term_by_enrolled AS (PARTITION BY student_id, term ORDER BY enrolled_at, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE seq_no = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        term,
        grade_status,
        score_bucket,
        enrolled_at_quartile,
        course_id_grade_status_max_score_bucket,
        distinct_student_id_in_course_id_grade_status,
        enrolled_at_percent_rank_2,
        rows_in_course_id_grade_status,
        prev_enrolled_at,
        rn,
        LEAD(enrolled_at) OVER w_term_by_rows_in_course_id_gr AS next_enrolled_at,
        LAST_VALUE(prev2_enrolled_at) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_enrolled_at), term ORDER BY prev2_enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prev2_enrolled_at,
        ROUND(AVG(rn) OVER w_prev2_enrolled_at_month_term, 2) AS prev2_enrolled_at_month_term_avg_rn,
        LEAD(prev_enrolled_at) OVER w_all_by_rn AS next_prev_enrolled_at
    FROM top_per_group
    WINDOW w_term_by_rows_in_course_id_gr AS (PARTITION BY term ORDER BY rows_in_course_id_grade_status DESC, id), w_prev2_enrolled_at_month_term AS (PARTITION BY MONTH(prev2_enrolled_at), term), w_all_by_rn AS (ORDER BY rn DESC NULLS FIRST, id)
)
SELECT
    id,
    student_id,
    term,
    score_bucket,
    enrolled_at_quartile,
    course_id_grade_status_max_score_bucket,
    enrolled_at_percent_rank_2,
    prev_enrolled_at,
    rn,
    next_enrolled_at,
    last_prev2_enrolled_at,
    prev2_enrolled_at_month_term_avg_rn,
    next_prev_enrolled_at,
    last_prev2_enrolled_at + INTERVAL 2 HOURS AS last_prev2_enrolled_at_plus_2h,
    enrolled_at_quartile - rn AS enrolled_at_quartile_minus_rn
FROM metrics
ORDER BY last_prev2_enrolled_at NULLS LAST
