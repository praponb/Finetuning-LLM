WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE student_id IS NOT NULL
),
combined_details AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_len,
        c.course_code,
        c.level AS course_level,
        st.student_name
    FROM prepared s
    LEFT JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(grade_status_len / NULLIF(SUM(grade_status_len) OVER (PARTITION BY grade_status, student_id), 0), 4) AS grade_status_len_share_of_grade_status_student_i,
        ROUND(AVG(grade_status_len) OVER (PARTITION BY grade_status, student_id), 2) AS grade_status_student_id_avg_grade_status_len,
        ROUND(PERCENT_RANK() OVER (ORDER BY score), 3) AS score_percent_rank,
        MIN(score) OVER w_grade_status_grade_status_le AS grade_status_grade_status_len_min_score,
        LAG(enrolled_at) OVER w_all_by_enrolled_at AS prev_enrolled_at,
        ROUND(grade_status_len - AVG(grade_status_len) OVER (PARTITION BY grade_status, student_id), 2) AS grade_status_len_vs_grade_status_student_id_avg,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at
    FROM combined_details
    WINDOW w_grade_status_grade_status_le AS (PARTITION BY grade_status, grade_status_len), w_all_by_enrolled_at AS (ORDER BY enrolled_at ASC, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        course_code,
        course_level,
        student_name,
        grade_status_len_share_of_grade_status_student_i,
        score_percent_rank,
        grade_status_len_vs_grade_status_student_id_avg,
        second_enrolled_at,
        MIN(score_percent_rank) OVER (PARTITION BY student_id, grade_status) AS student_id_grade_status_min_score_percent_rank,
        SUM(grade_status_len) OVER (PARTITION BY student_id, grade_status) AS student_id_grade_status_sum_grade_status_len,
        ROUND(PERCENT_RANK() OVER w_all_by_grade_status_grade_st, 3) AS grade_status_grade_status_len_min_score_percent_,
        MAX(grade_status_len) OVER (ORDER BY grade_status_grade_status_len_min_score, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS grade_status_len_max_4,
        MAX(score_percent_rank) OVER w_student_id_grade_status AS student_id_grade_status_max_score_percent_rank,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY student_id, term ORDER BY grade_status_len_vs_grade_status_student_id_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_2,
        FIRST_VALUE(grade_status_len) IGNORE NULLS OVER (PARTITION BY student_id, term ORDER BY grade_status_len_vs_grade_status_student_id_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_grade_status_len
    FROM with_windows
    WINDOW w_all_by_grade_status_grade_st AS (ORDER BY grade_status_grade_status_len_min_score, id), w_student_id_grade_status AS (PARTITION BY student_id, grade_status)
),
totals AS (
    -- Summarise per group
    SELECT
        term,
        SUM(CASE WHEN LOWER(student_name) LIKE '%no%' THEN student_id_grade_status_sum_grade_status_len ELSE 0 END) AS student_id_grade_status_sum_grade_status_len_low,
        COUNT(DISTINCT term) AS distinct_terms,
        ROUND(AVG(student_id_grade_status_min_score_percent_rank), 2) AS avg_student_id_grade_status_min_score_percent_ra,
        ROUND(SUM(student_id_grade_status_min_score_percent_rank) / NULLIF(SUM(first_known_grade_status_len), 0), 3) AS student_id_grade_status_min_score_percent_rank_t
    FROM windowed
    WHERE student_id >= 5
    GROUP BY term
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(distinct_terms) OVER (PARTITION BY distinct_terms) AS distinct_terms_max_distinct_terms,
        ROUND(AVG(distinct_terms) OVER (PARTITION BY term, distinct_terms ORDER BY student_id_grade_status_min_score_percent_rank_t DESC, term ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS distinct_terms_avg_2,
        COUNT(*) OVER (PARTITION BY term, distinct_terms ORDER BY student_id_grade_status_sum_grade_status_len_low ASC NULLS LAST, term ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM totals
)
SELECT
    term,
    student_id_grade_status_sum_grade_status_len_low,
    distinct_terms,
    student_id_grade_status_min_score_percent_rank_t,
    distinct_terms_max_distinct_terms,
    distinct_terms_avg_2,
    running_count
FROM windowed_2
ORDER BY student_id_grade_status_sum_grade_status_len_low
