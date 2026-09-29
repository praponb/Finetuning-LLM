WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT_WS(': ', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE term IN ('2025T2', '2025T1', '2024T2')
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
        )
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        LAG(score) OVER (ORDER BY score, id) AS prev_score,
        MIN(score) OVER (PARTITION BY grade_status) AS grade_status_min_score,
        ROUND(score / NULLIF(SUM(score) OVER w_grade_status, 0), 4) AS score_share_of_grade_status,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status), 2) AS score_vs_grade_status_avg,
        SUM(score) OVER (PARTITION BY grade_status) AS grade_status_sum_score
    FROM filtered
    WINDOW w_grade_status AS (PARTITION BY grade_status)
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        ROUND(AVG(score), 2) AS avg_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        e.avg_enrollment_score
    FROM ranked s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(grade_status_min_score) OVER (PARTITION BY grade_status ORDER BY grade_status_sum_score, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS grade_status_min_score_avg_6_prior,
        MIN(score_share_of_grade_status) OVER w_grade_status AS grade_status_min_score_share_of_grade_status,
        FIRST_VALUE(score_share_of_grade_status) IGNORE NULLS OVER (PARTITION BY term, course_id ORDER BY score_vs_grade_status_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score_share_of_grade_status,
        ROUND(avg_enrollment_score - AVG(avg_enrollment_score) OVER (PARTITION BY grade_status), 2) AS avg_enrollment_score_vs_grade_status_avg
    FROM profiled
    WINDOW w_grade_status AS (PARTITION BY grade_status)
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.rows_in_grade_status,
        s.prev_score,
        s.grade_status_min_score,
        s.score_share_of_grade_status,
        s.score_vs_grade_status_avg,
        s.grade_status_sum_score,
        s.last_enrollment_enrolled_at,
        s.avg_enrollment_score,
        s.grade_status_min_score_avg_6_prior,
        s.grade_status_min_score_share_of_grade_status,
        s.first_known_score_share_of_grade_status,
        c.credits AS course_credits,
        c.level AS course_level
    FROM with_trends s
    JOIN courses c ON c.id = s.course_id AND c.credits < 4
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    rows_in_grade_status,
    prev_score,
    grade_status_min_score,
    score_share_of_grade_status,
    score_vs_grade_status_avg,
    last_enrollment_enrolled_at,
    avg_enrollment_score,
    grade_status_min_score_avg_6_prior,
    course_credits,
    course_level,
    LEAST(prev_score, score) AS least_prev_score_score,
    ABS(grade_status_sum_score - 350.0) AS grade_status_sum_score_distance
FROM with_details_course
ORDER BY grade_status_min_score DESC, score DESC
