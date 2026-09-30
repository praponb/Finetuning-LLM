WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE term = '2025T2'
),
joined AS (
    -- Attach student attributes
    SELECT s.*, st.enrolled_on AS student_enrolled_on
    FROM cleaned s
    LEFT JOIN students st ON st.id = s.student_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(score) OVER (PARTITION BY grade_status, term ORDER BY score NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_sum_3,
        ROUND(CUME_DIST() OVER (ORDER BY score ASC, id), 3) AS score_cume_dist,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY term ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        NTH_VALUE(student_enrolled_on, 2) OVER (PARTITION BY term ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_enrolled_on,
        DENSE_RANK() OVER (PARTITION BY term ORDER BY score DESC, id) AS score_dense_rank,
        SUM(score) OVER w_student_id AS student_id_sum_score,
        NTILE(3) OVER w_all_by_score_2 AS score_tercile,
        score - LAG(score) OVER w_grade_status_by_score AS score_change
    FROM joined
    WINDOW w_student_id AS (PARTITION BY student_id), w_all_by_score_2 AS (ORDER BY score NULLS LAST, id), w_grade_status_by_score AS (PARTITION BY grade_status ORDER BY score, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        term,
        grade_status,
        student_enrolled_on,
        first_known_score,
        second_student_enrolled_on,
        score_tercile,
        score_change,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref,
        UPPER(TRIM(grade_status)) AS grade_status_clean,
        score_tercile + first_known_score AS score_tercile_plus_first_known_score
    FROM windowed
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_first_known_score_tie, 3) AS first_known_score_percent_rank,
        SUM(score_change) OVER (PARTITION BY score_tercile, grade_status ORDER BY second_student_enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score_change,
        DENSE_RANK() OVER w_grade_status_by_student_enro AS student_enrolled_on_dense_rank,
        ROUND(AVG(first_known_score) OVER (ORDER BY first_known_score, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS first_known_score_avg_7,
        LAG(score_change) OVER (PARTITION BY grade_status ORDER BY student_enrolled_on, id) AS prev_score_change,
        FIRST_VALUE(second_student_enrolled_on) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY student_enrolled_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_student_enrolled_on,
        MAX(score_tercile) OVER (PARTITION BY score_tercile, term) AS score_tercile_term_max_score_tercile,
        COUNT(*) OVER (PARTITION BY score_tercile, term) AS rows_in_score_tercile_term
    FROM derived
    WINDOW w_all_by_first_known_score_tie AS (ORDER BY first_known_score), w_grade_status_by_student_enro AS (PARTITION BY grade_status ORDER BY student_enrolled_on, id)
)
-- Final projection
SELECT
    id,
    term,
    grade_status,
    first_known_score,
    second_student_enrolled_on,
    score_change,
    grade_status_clean,
    score_tercile_plus_first_known_score,
    first_known_score_percent_rank,
    student_enrolled_on_dense_rank,
    first_known_score_avg_7,
    first_known_second_student_enrolled_on,
    ADD_MONTHS(student_enrolled_on, 12) AS student_enrolled_on_plus_12m,
    FLOOR(running_score_change / 250) * 250 AS running_score_change_bucket
FROM metrics
ORDER BY student_enrolled_on_plus_12m, second_student_enrolled_on DESC
