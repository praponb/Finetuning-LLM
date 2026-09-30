WITH valid AS (
    -- Scope the enrollments rows
    SELECT *
    FROM enrollments
    WHERE student_id IS NOT NULL
),
joined_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        c.level AS course_level,
        c.dept AS course_dept,
        st.cohort AS student_cohort,
        st.major AS student_major
    FROM valid s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
    WHERE c.dept IN ('Bio', 'Math')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        score,
        grade_status,
        course_level,
        SUM(score) OVER (PARTITION BY grade_status, term ORDER BY score ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS score_sum_3_prior,
        NTILE(3) OVER w_course_dept_student_cohort_b AS score_tercile,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        score - LAG(score) OVER w_course_dept_student_cohort_b AS score_change,
        SUM(score) OVER (PARTITION BY course_dept, student_cohort ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_sum_1_prior,
        ROUND(score - AVG(score) OVER w_course_id, 2) AS score_vs_course_id_avg,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_dept, student_cohort ORDER BY score, id), 3) AS score_percent_rank,
        LEAD(score) OVER (ORDER BY score, id) AS next_score,
        ROUND(AVG(score) OVER (ORDER BY score, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_avg_2_prior,
        MIN(score) OVER (PARTITION BY grade_status, term ORDER BY score ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        SUM(score) OVER (PARTITION BY course_dept, student_cohort ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY grade_status, term ORDER BY score ASC NULLS LAST, id), 3) AS score_percent_rank_2
    FROM joined_course
    WINDOW w_course_dept_student_cohort_b AS (PARTITION BY course_dept, student_cohort ORDER BY score, id), w_course_id AS (PARTITION BY course_id)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(score_vs_course_id_avg - 20.0) AS score_vs_course_id_avg_distance
    FROM sequenced
)
-- Final projection
SELECT
    id,
    student_id,
    score,
    grade_status,
    course_level,
    score_sum_3_prior,
    score_tercile,
    score_share_of_course_id,
    score_sum_1_prior,
    rows_in_course_id,
    score_percent_rank,
    next_score,
    min_score_so_far,
    score_percent_rank_2,
    score_vs_course_id_avg_distance,
    SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix
FROM derived
ORDER BY grade_status_prefix DESC, score_share_of_course_id DESC
