WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 57.3) AS score_distance,
        ABS(score - 38.1) AS score_distance_2
    FROM enrollments
    WHERE term IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, st.student_name, st.gpa AS student_gpa
    FROM scoped_enrollments s
    JOIN students st ON st.id = s.student_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        term,
        grade_status,
        enrolled_at,
        score_distance,
        student_name,
        student_gpa,
        LAST_VALUE(score_distance_2) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_distance_2,
        ROUND(AVG(score_distance_2) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS score_distance_2_avg_3,
        ROUND(score_distance - AVG(score_distance) OVER (PARTITION BY term), 2) AS score_distance_vs_term_avg,
        ROUND(score_distance_2 / NULLIF(SUM(score_distance_2) OVER (PARTITION BY term), 0), 4) AS score_distance_2_share_of_term,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        SUM(score_distance) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_distance_sum_1_prior,
        LAG(enrolled_at) OVER w_term_by_enrolled_at AS prev_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY term ORDER BY enrolled_at) AS enrolled_at_dense_rank
    FROM joined
    WINDOW w_term_by_enrolled_at AS (PARTITION BY term ORDER BY enrolled_at, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        term,
        grade_status,
        enrolled_at,
        score_distance,
        student_name,
        student_gpa,
        last_score_distance_2,
        score_distance_2_avg_3,
        score_distance_vs_term_avg,
        score_distance_2_share_of_term,
        rows_in_term,
        score_distance_sum_1_prior,
        prev_enrolled_at
    FROM with_trends
    WHERE enrolled_at_dense_rank = 1
      AND last_score_distance_2 >= 30.0
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(score_distance_vs_term_avg) IGNORE NULLS OVER (ORDER BY prev_enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score_distance_vs_term_avg,
        ROUND(CUME_DIST() OVER (ORDER BY prev_enrolled_at, id), 3) AS prev_enrolled_at_cume_dist,
        SUM(score_distance_vs_term_avg) OVER (ORDER BY prev_enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score_distance_vs_term_avg,
        ROUND(score_distance - AVG(score_distance) OVER (PARTITION BY term), 2) AS score_distance_vs_term_avg_2,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term_2
    FROM top_rows
)
SELECT
    id,
    term,
    grade_status,
    enrolled_at,
    score_distance,
    student_name,
    last_score_distance_2,
    score_distance_2_avg_3,
    score_distance_vs_term_avg,
    score_distance_2_share_of_term,
    rows_in_term,
    score_distance_sum_1_prior,
    prev_enrolled_at,
    prev_enrolled_at_cume_dist,
    running_score_distance_vs_term_avg,
    score_distance_vs_term_avg_2,
    rows_in_term_2
FROM windowed
