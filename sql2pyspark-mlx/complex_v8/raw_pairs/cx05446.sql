WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CASE WHEN grade_status IN ('WITHDRAWN', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'PASS' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE grade_status IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(score, 2) OVER (PARTITION BY term, course_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        DENSE_RANK() OVER w_course_id_student_id_by_scor AS score_dense_rank,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        MIN(score) OVER (PARTITION BY term, grade_status) AS term_grade_status_min_score,
        ROUND(score - AVG(score) OVER w_grade_status, 2) AS score_vs_grade_status_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term, course_id ORDER BY score DESC), 3) AS score_percent_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status), 0), 4) AS score_share_of_grade_status,
        ROUND(AVG(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior,
        SUM(score) OVER (PARTITION BY grade_status) AS grade_status_sum_score,
        SIZE(COLLECT_SET(grade_status_group) OVER (PARTITION BY term, grade_status)) AS distinct_grade_status_group_in_term_grade_status,
        ROW_NUMBER() OVER (ORDER BY score DESC, id) AS score_seq,
        DENSE_RANK() OVER (PARTITION BY course_id, student_id ORDER BY score) AS score_dense_rank_2
    FROM valid
    WINDOW w_course_id_student_id_by_scor AS (PARTITION BY course_id, student_id ORDER BY score, id), w_grade_status AS (PARTITION BY grade_status)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        term,
        grade_status,
        grade_status_group,
        second_score,
        score_dense_rank,
        rows_in_grade_status,
        term_grade_status_min_score,
        score_vs_grade_status_avg,
        score_percent_rank,
        score_share_of_grade_status,
        distinct_grade_status_group_in_term_grade_status,
        score_seq,
        NTH_VALUE(second_score, 2) OVER (PARTITION BY course_id ORDER BY score_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_score,
        ROUND(AVG(grade_status_sum_score) OVER w_distinct_grade_status_group, 2) AS distinct_grade_status_group_in_term_grade_status_2,
        NTH_VALUE(score_percent_rank, 2) OVER (PARTITION BY course_id ORDER BY score_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_percent_rank,
        LAST_VALUE(term_grade_status_min_score) OVER (ORDER BY distinct_grade_status_group_in_term_grade_status NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_term_grade_status_min_score,
        ROUND(AVG(term_grade_status_min_score) OVER (PARTITION BY course_id ORDER BY score_percent_rank, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS term_grade_status_min_score_avg_2,
        DENSE_RANK() OVER (PARTITION BY course_id ORDER BY score_percent_rank) AS score_percent_rank_dense_rank
    FROM ranked
    WINDOW w_distinct_grade_status_group AS (PARTITION BY distinct_grade_status_group_in_term_grade_status)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        grade_status_group,
        distinct_grade_status_group_in_term_grade_status,
        score_percent_rank_dense_rank,
        SUM(score_seq) FILTER (WHERE term <> '2024T2') AS score_seq_term_2024t2,
        MAX(rows_in_grade_status) AS max_rows_in_grade_status,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
    FROM sequenced
    GROUP BY grade_status_group, distinct_grade_status_group_in_term_grade_status, score_percent_rank_dense_rank
    HAVING COUNT(*) >= 2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        grade_status_group,
        distinct_grade_status_group_in_term_grade_status,
        score_percent_rank_dense_rank,
        score_seq_term_2024t2,
        max_rows_in_grade_status,
        cnt,
        grade_status_list,
        ROUND(PERCENT_RANK() OVER (PARTITION BY score_percent_rank_dense_rank, grade_status_group ORDER BY score_seq_term_2024t2), 3) AS score_seq_term_2024t2_percent_rank,
        MAX(score_seq_term_2024t2) OVER (PARTITION BY cnt, distinct_grade_status_group_in_term_grade_status) AS cnt_distinct_grade_status_group_in_term_grade_st,
        COUNT(*) OVER w_cnt_distinct_grade_status_gr AS rows_in_cnt_distinct_grade_status_group_in_term_
    FROM totals
    WINDOW w_cnt_distinct_grade_status_gr AS (PARTITION BY cnt, distinct_grade_status_group_in_term_grade_status)
)
SELECT
    grade_status_group,
    score_percent_rank_dense_rank,
    max_rows_in_grade_status,
    cnt,
    score_seq_term_2024t2_percent_rank,
    cnt_distinct_grade_status_group_in_term_grade_st,
    rows_in_cnt_distinct_grade_status_group_in_term_,
    COALESCE(score_seq_term_2024t2, 0) AS score_seq_term_2024t2_filled,
    COALESCE(grade_status_group, 'UNKNOWN') AS grade_status_group_or_unknown
FROM with_trends
ORDER BY score_percent_rank_dense_rank, grade_status_group_or_unknown NULLS LAST
