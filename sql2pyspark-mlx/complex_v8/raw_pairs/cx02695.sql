WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM enrollments
    WHERE grade_status NOT IN ('PASS', 'FAIL')
      AND score >= 34.2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        ROUND(PERCENT_RANK() OVER (ORDER BY score DESC), 3) AS score_percent_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_score_ties_2, 3) AS score_percent_rank_2,
        ROUND(CUME_DIST() OVER (ORDER BY score DESC, id), 3) AS score_cume_dist,
        ROUND(score - AVG(score) OVER (PARTITION BY course_id), 2) AS score_vs_course_id_avg,
        ROW_NUMBER() OVER (ORDER BY score, id) AS seq_no,
        SUM(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS score_sum_3_prior,
        score - LAG(score) OVER (PARTITION BY course_id ORDER BY score ASC, id) AS score_change,
        ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY score ASC, id) AS score_seq
    FROM recent_enrollments
    WINDOW w_all_by_score_ties_2 AS (ORDER BY score), w_all_by_score_ties AS (ORDER BY score DESC), w_course_id AS (PARTITION BY course_id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(score_cume_dist, score_sum_3_prior) AS least_score_cume_dist_score_sum_3_prior,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        INITCAP(TRIM(term)) AS term_clean
    FROM with_windows
    WHERE score_vs_course_id_avg >= -50.0
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    score_percent_rank,
    score_percent_rank_2,
    score_cume_dist,
    seq_no,
    score_sum_3_prior,
    score_change,
    score_seq,
    least_score_cume_dist_score_sum_3_prior,
    term_clean,
    SUBSTRING(TRIM(term_clean), 1, 3) AS term_clean_prefix
FROM derived
ORDER BY course_id, score_percent_rank_2 NULLS LAST
