WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(score, 2) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        COUNT(*) OVER (PARTITION BY course_id, grade_status) AS rows_in_course_id_grade_status,
        FIRST_VALUE(score) OVER (PARTITION BY grade_status, student_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        ROUND(score - AVG(score) OVER w_course_id_grade_status, 2) AS score_vs_course_id_grade_status_avg,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY score DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        RANK() OVER (PARTITION BY grade_status, course_id ORDER BY enrolled_at NULLS LAST, id) AS enrolled_at_rank,
        MIN(score) OVER w_course_id_grade_status AS course_id_grade_status_min_score
    FROM prepared
    WINDOW w_course_id_grade_status AS (PARTITION BY course_id, grade_status)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        enrolled_at_rank,
        student_id,
        SUM(course_id_grade_status_min_score) AS total_course_id_grade_status_min_score,
        PERCENTILE_APPROX(minutes_since_prev_enrolled_at, 0.5) AS median_minutes_since_prev_enrolled_at,
        COUNT(*) FILTER (WHERE grade_status NOT IN ('INCOMPLETE', 'WITHDRAWN')) AS n_grade_status_not_in_incomple,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        ROUND(AVG(second_score), 2) AS avg_second_score
    FROM sequenced
    WHERE student_id IS NOT NULL
    GROUP BY course_id, enrolled_at_rank, student_id
)
SELECT
    course_id,
    enrolled_at_rank,
    student_id,
    median_minutes_since_prev_enrolled_at,
    n_rows,
    distinct_course_ids,
    avg_second_score
FROM summary
WHERE n_rows < 8
ORDER BY median_minutes_since_prev_enrolled_at DESC
