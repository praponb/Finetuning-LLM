WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('HOUR', enrolled_at) AS enrolled_at_hour,
        CASE WHEN score >= 79.3 THEN 'large' WHEN score >= 26.2 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND student_id >= 15
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(score / NULLIF(SUM(score) OVER w_enrolled_at_month, 0), 4) AS score_share_of_enrolled_at_month,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY score DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY score DESC NULLS FIRST, id) AS score_dense_rank,
        COUNT(*) OVER w_enrolled_at_month AS rows_in_enrolled_at_month,
        ROUND(AVG(score) OVER (PARTITION BY MONTH(enrolled_at)), 2) AS enrolled_at_month_avg_score,
        ROUND(AVG(score) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY score DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS score_avg_1_prior
    FROM valid_enrollments
    WINDOW w_enrolled_at_month AS (PARTITION BY MONTH(enrolled_at))
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(score_avg_1_prior) OVER (PARTITION BY course_id) AS course_id_max_score_avg_1_prior,
        NTH_VALUE(score, 2) OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at_hour NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        NTH_VALUE(enrolled_at_month_avg_score, 2) OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at_hour NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_month_avg_score
    FROM with_windows
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        ROUND(PERCENT_RANK() OVER (PARTITION BY score_tier, term ORDER BY enrolled_at_hour DESC NULLS FIRST), 3) AS enrolled_at_hour_percent_rank,
        DENSE_RANK() OVER (ORDER BY score_share_of_enrolled_at_month ASC) AS score_share_of_enrolled_at_month_dense_rank
    FROM with_windows_2
),
with_details AS (
    -- Join courses
    SELECT
        s.*,
        c.level AS course_level,
        c.credits AS course_credits,
        st.student_name,
        st.enrolled_on AS student_enrolled_on
    FROM sequenced s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
    WHERE c.level = 'core'
),
combined_details AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.score_tier,
        s.score_share_of_enrolled_at_month,
        s.running_count,
        s.score_dense_rank,
        s.rows_in_enrolled_at_month,
        s.enrolled_at_month_avg_score,
        s.score_avg_1_prior,
        s.course_id_max_score_avg_1_prior,
        s.second_score,
        s.rows_in_term,
        s.course_level,
        s.student_name,
        st.is_international AS student_is_international,
        st.major AS student_major,
        c.level AS course_level_2,
        c.course_code
    FROM with_details s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
with_parent_student AS (
    -- Join students
    SELECT
        s.*,
        st.student_name AS student_student_name,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort,
        c.credits AS course_credits,
        c.dept AS course_dept
    FROM combined_details s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
)
-- Subtotals with CUBE
SELECT
    course_credits,
    course_level,
    student_major,
    GROUPING_ID() AS grouping_level,
    SUM(score_share_of_enrolled_at_month) AS total_score_share_of_enrolled_at_month,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT student_cohort) AS distinct_student_cohorts
FROM with_parent_student
GROUP BY CUBE(course_credits, course_level, student_major)
ORDER BY course_credits NULLS LAST, course_level NULLS LAST, student_major NULLS LAST
