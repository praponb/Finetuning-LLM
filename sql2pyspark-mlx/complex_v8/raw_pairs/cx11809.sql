WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE course_id <= 9
),
joined_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        c.credits AS course_credits,
        c.level AS course_level,
        st.major AS student_major,
        st.cohort AS student_cohort
    FROM recent s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        course_credits,
        course_level,
        student_major,
        student_cohort,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        ROUND(AVG(course_credits) OVER (ORDER BY course_credits, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS course_credits_avg_2_prior,
        SIZE(COLLECT_SET(course_level) OVER (PARTITION BY course_id)) AS distinct_course_level_in_course_id,
        NTILE(10) OVER (PARTITION BY student_cohort ORDER BY course_credits, id) AS course_credits_decile,
        LEAD(course_credits, 1, 0) OVER (ORDER BY course_credits, id) AS next_course_credits
    FROM joined_course
    WINDOW w_course_id AS (PARTITION BY course_id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY student_cohort, course_credits_decile ORDER BY course_credits_decile DESC), 3) AS course_credits_decile_cume_dist,
        DENSE_RANK() OVER (ORDER BY distinct_course_level_in_course_id DESC, id) AS distinct_course_level_in_course_id_dense_rank,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id_2,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_id)) AS distinct_student_id_in_course_id,
        MAX(course_credits_avg_2_prior) OVER (PARTITION BY course_id) AS course_id_max_course_credits_avg_2_prior,
        DENSE_RANK() OVER (ORDER BY distinct_course_level_in_course_id DESC) AS distinct_course_level_in_course_id_dense_rank_2,
        LAST_VALUE(course_credits_decile) IGNORE NULLS OVER (PARTITION BY student_cohort, course_credits_decile ORDER BY course_credits_decile DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_course_credits_decile
    FROM with_windows
)
SELECT
    id,
    course_id,
    term,
    course_credits,
    course_level,
    rows_in_course_id,
    course_credits_avg_2_prior,
    distinct_course_level_in_course_id,
    course_credits_decile,
    next_course_credits,
    course_credits_decile_cume_dist,
    distinct_course_level_in_course_id_dense_rank,
    rows_in_course_id_2,
    course_id_max_course_credits_avg_2_prior,
    last_known_course_credits_decile,
    LEAST(course_credits_decile, rows_in_course_id_2) AS least_course_credits_decile_rows_in_course_id_2
FROM windowed
WHERE distinct_course_level_in_course_id >= 4
ORDER BY rows_in_course_id_2 DESC
