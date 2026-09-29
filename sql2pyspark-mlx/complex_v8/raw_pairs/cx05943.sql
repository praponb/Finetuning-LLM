WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix,
        LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE course_id IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS enrolled_at_quintile,
        COUNT(*) OVER (PARTITION BY term, student_id) AS rows_in_term_student_id,
        MAX(score) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_max_2_prior,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at
    FROM prepared_enrollments
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at DESC NULLS FIRST, id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', second_enrolled_at) AS second_enrolled_at_month,
        term,
        student_id,
        MIN(rows_in_term_student_id) AS min_rows_in_term_student_id,
        MAX(second_enrolled_at) AS last_second_enrolled_at,
        MAX(score_max_2_prior) AS max_score_max_2_prior,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
    FROM sequenced
    GROUP BY DATE_TRUNC('MONTH', second_enrolled_at), term, student_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(last_second_enrolled_at) OVER (PARTITION BY student_id, term ORDER BY last_second_enrolled_at DESC, second_enrolled_at_month, term, student_id) AS next_last_second_enrolled_at,
        ROUND((UNIX_TIMESTAMP(last_second_enrolled_at) - UNIX_TIMESTAMP(LAG(last_second_enrolled_at) OVER w_all_by_min_rows_in_term_stud)) / 60, 1) AS minutes_since_prev_last_second_enrolled_at,
        ROUND(PERCENT_RANK() OVER w_all_by_min_rows_in_term_stud_2, 3) AS min_rows_in_term_student_id_percent_rank,
        n_rows - LAG(n_rows) OVER w_all_by_min_rows_in_term_stud AS n_rows_change
    FROM per_group
    WINDOW w_all_by_min_rows_in_term_stud AS (ORDER BY min_rows_in_term_student_id, second_enrolled_at_month, term, student_id), w_all_by_min_rows_in_term_stud_2 AS (ORDER BY min_rows_in_term_student_id)
),
per_label AS (
    -- One row per grade_status_list_item
    SELECT
        s.second_enrolled_at_month,
        s.term,
        s.student_id,
        s.min_rows_in_term_student_id,
        s.last_second_enrolled_at,
        s.max_score_max_2_prior,
        s.n_rows,
        s.next_last_second_enrolled_at,
        s.minutes_since_prev_last_second_enrolled_at,
        s.min_rows_in_term_student_id_percent_rank,
        s.n_rows_change,
        t.grade_status_list_item
    FROM with_trends s
    LATERAL VIEW EXPLODE(s.grade_status_list) t AS grade_status_list_item
)
-- Final projection
SELECT
    second_enrolled_at_month,
    min_rows_in_term_student_id,
    last_second_enrolled_at,
    max_score_max_2_prior,
    n_rows,
    next_last_second_enrolled_at,
    min_rows_in_term_student_id_percent_rank,
    n_rows_change,
    grade_status_list_item,
    LEAST(n_rows_change, max_score_max_2_prior) AS least_n_rows_change_max_score_max_2_prior
FROM per_label
ORDER BY max_score_max_2_prior DESC, last_second_enrolled_at DESC NULLS FIRST
