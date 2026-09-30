WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek,
        COALESCE(score, 0.0) AS score_filled,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE score < 59.8
      AND grade_status IS NOT NULL
),
joined_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_dayofweek,
        s.score_filled,
        s.grade_status_term_label,
        c.level AS course_level,
        c.course_code
    FROM scoped_enrollments s
    JOIN courses c ON c.id = s.course_id
    WHERE c.level NOT IN ('advanced', 'core')
),
student_id_stats AS (
    -- Median score per student_id
    SELECT student_id, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM joined_course
    GROUP BY student_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM joined_course s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score >= g.median_score
),
enriched_course AS (
    -- Enrich with dimension data
    SELECT s.*, c.dept AS course_dept, c.credits AS course_credits
    FROM outliers s
    JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        score,
        enrolled_at,
        enrolled_at_dayofweek,
        score_filled,
        score_minus_median,
        course_dept,
        course_credits,
        LEAD(enrolled_at_dayofweek) OVER (PARTITION BY enrolled_at_dayofweek ORDER BY enrolled_at DESC, id) AS next_enrolled_at_dayofweek,
        score_minus_median - LAG(score_minus_median) OVER (PARTITION BY enrolled_at_dayofweek ORDER BY enrolled_at DESC, id) AS score_minus_median_change,
        COUNT(*) OVER (PARTITION BY TO_DATE(enrolled_at)) AS rows_in_enrolled_at_to_date,
        SUM(enrolled_at_dayofweek) OVER (PARTITION BY enrolled_at_dayofweek ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_enrolled_at_dayofweek,
        MAX(score_minus_median) OVER (PARTITION BY enrolled_at_dayofweek ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_minus_median_so_far
    FROM enriched_course
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(rows_in_enrolled_at_to_date) OVER (PARTITION BY student_id) AS student_id_min_rows_in_enrolled_at_to_date,
        ROUND(rows_in_enrolled_at_to_date - AVG(rows_in_enrolled_at_to_date) OVER (PARTITION BY student_id), 2) AS rows_in_enrolled_at_to_date_vs_student_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY enrolled_at_dayofweek ASC), 3) AS enrolled_at_dayofweek_percent_rank,
        DENSE_RANK() OVER (PARTITION BY enrolled_at_dayofweek ORDER BY max_score_minus_median_so_far) AS max_score_minus_median_so_far_dense_rank
    FROM metrics
)
SELECT
    id,
    student_id,
    enrolled_at,
    enrolled_at_dayofweek,
    score_filled,
    score_minus_median,
    course_dept,
    course_credits,
    next_enrolled_at_dayofweek,
    rows_in_enrolled_at_to_date,
    running_enrolled_at_dayofweek,
    max_score_minus_median_so_far,
    rows_in_enrolled_at_to_date_vs_student_id_avg,
    enrolled_at_dayofweek_percent_rank,
    max_score_minus_median_so_far_dense_rank,
    GREATEST(course_credits, score) AS greatest_course_credits_score,
    LOWER(TRIM(course_dept)) AS course_dept_clean
FROM with_windows
ORDER BY course_dept_clean DESC, student_id
