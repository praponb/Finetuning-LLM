WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-18 10:00:00'
      AND course_id IS NOT NULL
),
combined_details_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        st.enrolled_on AS student_enrolled_on,
        st.is_international AS student_is_international,
        c.credits AS course_credits
    FROM base_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(score, 3) OVER (ORDER BY course_credits, id) AS next3_score,
        ROW_NUMBER() OVER (PARTITION BY course_credits, course_id ORDER BY course_credits, id) AS rn,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER w_course_id, 0), 4) AS course_credits_share_of_course_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at ASC), 3) AS enrolled_at_percent_rank,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        ROUND(AVG(score) OVER (PARTITION BY course_credits, course_id ORDER BY course_credits, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY enrolled_at ASC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_credits, course_id ORDER BY course_credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY course_credits, course_id ORDER BY course_credits, id))) / 60, 1) AS minutes_since_prev_enrolled_at_3,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY course_credits, course_id ORDER BY course_credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        LAG(enrolled_at, 3) OVER w_course_credits_course_id_by AS prev3_enrolled_at,
        ROUND(AVG(score) OVER (PARTITION BY course_id), 2) AS course_id_avg_score,
        DENSE_RANK() OVER w_all_by_enrolled_at_ties AS enrolled_at_dense_rank
    FROM combined_details_student
    WINDOW w_course_id AS (PARTITION BY course_id), w_all_by_enrolled_at AS (ORDER BY enrolled_at ASC, id), w_course_credits_course_id_by AS (PARTITION BY course_credits, course_id ORDER BY course_credits, id), w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at ASC)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        enrolled_at,
        student_enrolled_on,
        course_credits,
        next3_score,
        course_credits_share_of_course_id,
        enrolled_at_percent_rank,
        rows_in_course_id,
        score_avg_3_prior,
        minutes_since_prev_enrolled_at,
        second_enrolled_at,
        minutes_since_prev_enrolled_at_3,
        first_enrolled_at,
        prev3_enrolled_at,
        course_id_avg_score,
        enrolled_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(enrolled_at) ORDER BY second_enrolled_at), 3) AS second_enrolled_at_cume_dist,
        ROUND(rows_in_course_id / NULLIF(SUM(rows_in_course_id) OVER (PARTITION BY rn, YEAR(second_enrolled_at)), 0), 4) AS rows_in_course_id_share_of_rn_second_enrolled_at,
        COUNT(*) OVER w_course_id_enrolled_at_dense AS rows_in_course_id_enrolled_at_dense_rank,
        LAG(next3_score, 2) OVER w_prev3_enrolled_at_month_by_m AS prev2_next3_score,
        ROUND(AVG(rows_in_course_id) OVER (PARTITION BY MONTH(prev3_enrolled_at) ORDER BY minutes_since_prev_enrolled_at_3, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS rows_in_course_id_avg_6_prior,
        FIRST_VALUE(rows_in_course_id) IGNORE NULLS OVER (PARTITION BY MONTH(prev3_enrolled_at) ORDER BY minutes_since_prev_enrolled_at_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rows_in_course_id
    FROM with_trends
    WINDOW w_course_id_enrolled_at_dense AS (PARTITION BY course_id, enrolled_at_dense_rank), w_prev3_enrolled_at_month_by_m AS (PARTITION BY MONTH(prev3_enrolled_at) ORDER BY minutes_since_prev_enrolled_at_3, id)
)
SELECT
    id,
    enrolled_at,
    next3_score,
    rows_in_course_id,
    minutes_since_prev_enrolled_at,
    second_enrolled_at,
    minutes_since_prev_enrolled_at_3,
    first_enrolled_at,
    prev3_enrolled_at,
    enrolled_at_dense_rank,
    rows_in_course_id_share_of_rn_second_enrolled_at,
    rows_in_course_id_enrolled_at_dense_rank,
    prev2_next3_score,
    rows_in_course_id_avg_6_prior,
    DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
    prev2_next3_score - course_id_avg_score AS prev2_next3_score_minus_course_id_avg_score
FROM metrics
ORDER BY next3_score DESC, enrolled_at_dense_rank NULLS LAST
