WITH filtered AS (
    -- Filter enrollments
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
    WHERE grade_status <> 'FAIL'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(grade_status_len) OVER (PARTITION BY grade_status_len, term ORDER BY grade_status_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_grade_status_len_so_far,
        LEAD(grade_status_len) OVER w_grade_status_by_grade_status AS next_grade_status_len,
        SUM(score) OVER (PARTITION BY term, YEAR(enrolled_at)) AS term_enrolled_at_year_sum_score,
        ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY grade_status_len, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER (PARTITION BY grade_status ORDER BY grade_status_len), 3) AS grade_status_len_percent_rank,
        ROUND(score - AVG(score) OVER (PARTITION BY term, YEAR(enrolled_at)), 2) AS score_vs_term_enrolled_at_year_avg,
        RANK() OVER (ORDER BY grade_status_len DESC) AS grade_status_len_rank,
        ROUND(AVG(grade_status_len) OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS grade_status_len_avg_1_prior,
        ROUND(AVG(grade_status_len) OVER w_student_id_grade_status_len, 2) AS student_id_grade_status_len_avg_grade_status_len,
        ROUND(CUME_DIST() OVER (ORDER BY grade_status_len DESC, id), 3) AS grade_status_len_cume_dist
    FROM filtered
    WINDOW w_grade_status_by_grade_status AS (PARTITION BY grade_status ORDER BY grade_status_len, id), w_student_id_grade_status_len AS (PARTITION BY student_id, grade_status_len)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    grade_status_len,
    min_grade_status_len_so_far,
    next_grade_status_len,
    term_enrolled_at_year_sum_score,
    seq_no,
    score_vs_term_enrolled_at_year_avg,
    grade_status_len_rank,
    grade_status_len_avg_1_prior,
    student_id_grade_status_len_avg_grade_status_len,
    grade_status_len_cume_dist,
    ROUND(next_grade_status_len / NULLIF(grade_status_len, 0), 3) AS next_grade_status_len_per_grade_status_len
FROM windowed
WHERE min_grade_status_len_so_far <= 85
ORDER BY student_id_grade_status_len_avg_grade_status_len DESC, id
LIMIT 5
