WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label,
        ROUND(score * 1.5, 2) AS score_adj
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-18 10:00:00'
),
with_parent AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_label,
        s.score_adj,
        c.credits AS course_credits
    FROM prepared s
    JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(score_adj) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_adj_avg_7,
        SUM(score_adj) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_adj_sum_3,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        FIRST_VALUE(score_adj) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score_adj,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY YEAR(enrolled_at))) AS distinct_course_id_in_enrolled_at_year,
        DENSE_RANK() OVER (PARTITION BY grade_status ORDER BY score DESC, id) AS score_dense_rank,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY course_credits ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        RANK() OVER w_all_by_course_credits_ties AS course_credits_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY YEAR(enrolled_at)), 0), 4) AS score_share_of_enrolled_at_year,
        COUNT(*) OVER w_enrolled_at_year AS rows_in_enrolled_at_year,
        LEAD(score, 3) OVER (ORDER BY course_credits ASC, id) AS next3_score,
        DENSE_RANK() OVER w_course_id_by_course_credits AS course_credits_dense_rank,
        LEAD(score, 2, 0) OVER w_all_by_course_credits AS next2_score,
        ROUND(AVG(score_adj) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_adj_avg_2,
        LEAD(score_adj, 2, 0) OVER (PARTITION BY grade_status ORDER BY score DESC, id) AS next2_score_adj
    FROM with_parent
    WINDOW w_all_by_course_credits_ties AS (ORDER BY course_credits ASC), w_enrolled_at_year AS (PARTITION BY YEAR(enrolled_at)), w_course_id_by_course_credits AS (PARTITION BY course_id ORDER BY course_credits DESC NULLS FIRST), w_all_by_course_credits AS (ORDER BY course_credits ASC, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        score,
        enrolled_at,
        enrolled_at_label,
        score_adj,
        course_credits,
        score_adj_avg_7,
        score_adj_sum_3,
        first_enrolled_at,
        first_score_adj,
        distinct_course_id_in_enrolled_at_year,
        score_dense_rank,
        course_credits_rank,
        rows_in_enrolled_at_year,
        next3_score,
        next2_score,
        score_adj_avg_2,
        DATEDIFF(DATE '2025-12-31', first_enrolled_at) AS days_since_first_enrolled_at,
        ROUND((UNIX_TIMESTAMP(last_known_enrolled_at) - UNIX_TIMESTAMP(enrolled_at)) / 3600, 1) AS hours_enrolled_at_to_last_known_enrolled_at,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM sequenced
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (ORDER BY days_since_first_enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(PERCENT_RANK() OVER (ORDER BY days_since_first_enrolled_at), 3) AS days_since_first_enrolled_at_percent_rank,
        SUM(score_adj) OVER (ORDER BY days_since_first_enrolled_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_adj_sum_7,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_credits_rank, score_dense_rank)) AS distinct_student_id_in_course_credits_rank_score,
        RANK() OVER (PARTITION BY student_id, MONTH(enrolled_at) ORDER BY next2_score DESC, id) AS next2_score_rank,
        SUM(next2_score) OVER w_course_credits_rank_score_de AS course_credits_rank_score_dense_rank_sum_next2_s,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(first_enrolled_at) ORDER BY enrolled_at ASC, id), 3) AS enrolled_at_cume_dist
    FROM scored
    WINDOW w_course_credits_rank_score_de AS (PARTITION BY course_credits_rank, score_dense_rank)
),
totals AS (
    -- Group-level totals
    SELECT
        score_dense_rank,
        next2_score_rank,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        MAX(first_score_adj) AS max_first_score_adj,
        SUM(running_count) AS total_running_count,
        MAX(enrolled_at) AS last_enrolled_at,
        COUNT(*) AS n_rows
    FROM with_windows
    GROUP BY score_dense_rank, next2_score_rank, DATE_TRUNC('MONTH', enrolled_at)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        score_dense_rank,
        next2_score_rank,
        enrolled_at_month,
        max_first_score_adj,
        total_running_count,
        last_enrolled_at,
        n_rows,
        SUM(next2_score_rank) OVER (PARTITION BY score_dense_rank ORDER BY max_first_score_adj DESC, score_dense_rank, next2_score_rank, enrolled_at_month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next2_score_rank_sum_2,
        COUNT(*) OVER (PARTITION BY score_dense_rank ORDER BY max_first_score_adj DESC, score_dense_rank, next2_score_rank, enrolled_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(total_running_count) OVER w_n_rows, 2) AS n_rows_avg_total_running_count
    FROM totals
    WINDOW w_n_rows AS (PARTITION BY n_rows)
)
-- Final projection
SELECT
    score_dense_rank,
    next2_score_rank,
    max_first_score_adj,
    total_running_count,
    last_enrolled_at,
    n_rows,
    n_rows_avg_total_running_count,
    n_rows + running_count AS n_rows_plus_running_count
FROM with_trends
WHERE total_running_count <= 66
ORDER BY next2_score_rank NULLS LAST, n_rows_plus_running_count DESC
