WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label,
        FLOOR(score / 25) * 25 AS score_bucket,
        CONCAT_WS('-', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE grade_status IN ('FAIL', 'PASS')
      AND score > 42.4
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM scoped_enrollments s
),
enrollment_stats_2 AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) FILTER (WHERE grade_status <> 'PASS') AS enrollment_grade_status_count,
        MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count,
        e.last_enrollment_enrolled_at
    FROM profiled s
    LEFT JOIN enrollment_stats_2 e ON e.course_id = s.course_id
),
joined AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at_label,
        s.enrollment_count,
        s.enrollment_grade_status_count,
        s.last_enrollment_enrolled_at,
        c.credits AS course_credits
    FROM with_stats s
    LEFT JOIN courses c ON c.id = s.course_id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(score * 1.5, 2) AS score_adj,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        LENGTH(TRIM(term)) AS term_len
    FROM joined
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(score_adj) OVER (PARTITION BY enrollment_grade_status_count, DATE_TRUNC('MONTH', last_enrollment_enrolled_at)) AS enrollment_grade_status_count_last_enrollment_en,
        LEAD(course_credits, 3) OVER (PARTITION BY student_id ORDER BY course_credits, id) AS next3_course_credits,
        enrollment_count - LAG(enrollment_count) OVER (PARTITION BY enrollment_grade_status_count ORDER BY enrollment_count, id) AS enrollment_count_change,
        DENSE_RANK() OVER w_enrollment_grade_status_coun_3 AS enrollment_count_dense_rank,
        LAST_VALUE(last_enrollment_enrolled_at) OVER (PARTITION BY enrollment_grade_status_count ORDER BY enrollment_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_enrollment_enrolled_at,
        LEAD(course_credits, 2) OVER w_term_len_by_score_adj AS next2_course_credits
    FROM derived
    WINDOW w_enrollment_grade_status_coun_3 AS (PARTITION BY enrollment_grade_status_count ORDER BY enrollment_count), w_term_len_by_score_adj AS (PARTITION BY term_len ORDER BY score_adj DESC NULLS FIRST, id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrollment_count,
        enrollment_grade_status_count,
        last_enrollment_enrolled_at,
        course_credits,
        score_adj,
        term_len,
        next3_course_credits,
        enrollment_count_change,
        enrollment_count_dense_rank,
        next2_course_credits,
        NTILE(10) OVER (ORDER BY course_credits DESC, id) AS course_credits_decile,
        RANK() OVER (PARTITION BY enrollment_grade_status_count ORDER BY score DESC) AS score_rank,
        COUNT(*) OVER (PARTITION BY enrollment_count, YEAR(last_enrollment_enrolled_at)) AS rows_in_enrollment_count_last_enrollment_enrolle,
        ROUND(CUME_DIST() OVER (ORDER BY course_credits DESC), 3) AS course_credits_cume_dist,
        SUM(score_adj) OVER (PARTITION BY YEAR(last_last_enrollment_enrolled_at) ORDER BY score DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_adj_sum_2_prior,
        LEAD(next3_course_credits, 2) OVER (ORDER BY course_credits DESC, id) AS next2_next3_course_credits
    FROM with_windows
)
SELECT
    id,
    student_id,
    score,
    enrollment_count,
    enrollment_grade_status_count,
    last_enrollment_enrolled_at,
    course_credits,
    score_adj,
    term_len,
    next3_course_credits,
    enrollment_count_change,
    enrollment_count_dense_rank,
    course_credits_decile,
    score_rank,
    rows_in_enrollment_count_last_enrollment_enrolle,
    course_credits_cume_dist,
    score_adj_sum_2_prior,
    next2_next3_course_credits,
    ROUND(course_credits_cume_dist / NULLIF(course_credits, 0), 3) AS course_credits_cume_dist_per_course_credits
FROM sequenced
ORDER BY enrollment_count_change NULLS LAST, score_adj_sum_2_prior NULLS LAST, id
LIMIT 20
