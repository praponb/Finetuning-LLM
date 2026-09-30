WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE score <= 59.7
      AND term IN ('2025T2', '2024T1')
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.score_bucket,
        s.enrolled_at_date,
        c.level AS course_level
    FROM recent s
    JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        term,
        grade_status,
        score_bucket,
        enrolled_at_date,
        course_level,
        SIZE(COLLECT_SET(course_level) OVER (PARTITION BY student_id, MONTH(enrolled_at_date))) AS distinct_course_level_in_student_id_enrolled_at_,
        LAG(score_bucket, 3) OVER w_term_by_enrolled_at_date AS prev3_score_bucket,
        FIRST_VALUE(enrolled_at_date) OVER (PARTITION BY term ORDER BY enrolled_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_date
    FROM with_parent_course
    WINDOW w_term_by_enrolled_at_date AS (PARTITION BY term ORDER BY enrolled_at_date, id)
),
grade_status_stats AS (
    -- Avg prev3_score_bucket per grade_status
    SELECT grade_status, AVG(prev3_score_bucket) AS avg_prev3_score_bucket
    FROM sequenced
    GROUP BY grade_status
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prev3_score_bucket,
        ROUND(s.prev3_score_bucket - g.avg_prev3_score_bucket, 2) AS prev3_score_bucket_minus_avg
    FROM sequenced s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.prev3_score_bucket > g.avg_prev3_score_bucket
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(prev3_score_bucket_minus_avg) OVER (PARTITION BY grade_status, distinct_course_level_in_student_id_enrolled_at_), 2) AS grade_status_distinct_course_level_in_student_id,
        DENSE_RANK() OVER (ORDER BY prev3_score_bucket DESC, id) AS prev3_score_bucket_dense_rank,
        prev3_score_bucket_minus_avg - LAG(prev3_score_bucket_minus_avg) OVER (PARTITION BY course_level ORDER BY prev3_score_bucket_minus_avg DESC, id) AS prev3_score_bucket_minus_avg_change,
        RANK() OVER w_course_level_by_prev3_score AS prev3_score_bucket_minus_avg_rank,
        DATEDIFF(enrolled_at_date, LAG(enrolled_at_date) OVER (PARTITION BY course_level ORDER BY prev3_score_bucket_minus_avg DESC, id)) AS days_since_prev_enrolled_at_date
    FROM compared
    WINDOW w_course_level_by_prev3_score AS (PARTITION BY course_level ORDER BY prev3_score_bucket_minus_avg DESC, id)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_course_level_in_student_id_enrolled_at_,
        ROUND(SUM(score_bucket) / NULLIF(SUM(distinct_course_level_in_student_id_enrolled_at_), 0), 3) AS score_bucket_to_distinct_course_level_in_student,
        ROUND(SUM(score_bucket) / NULLIF(SUM(prev3_score_bucket), 0), 3) AS score_bucket_to_prev3_score_bucket_ratio,
        SUM(prev3_score_bucket_dense_rank) AS total_prev3_score_bucket_dense_rank,
        MIN(first_enrolled_at_date) AS first_first_enrolled_at_date,
        SUM(CASE WHEN grade_status = 'WITHDRAWN' THEN days_since_prev_enrolled_at_date ELSE 0 END) AS days_since_prev_enrolled_at_date_grade_status_wi
    FROM metrics
    WHERE grade_status IS NOT NULL
    GROUP BY distinct_course_level_in_student_id_enrolled_at_
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(first_first_enrolled_at_date, 2) OVER (PARTITION BY distinct_course_level_in_student_id_enrolled_at_ ORDER BY score_bucket_to_prev3_score_bucket_ratio, distinct_course_level_in_student_id_enrolled_at_ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_first_enrolled_at_date,
        LAST_VALUE(first_first_enrolled_at_date) IGNORE NULLS OVER (ORDER BY total_prev3_score_bucket_dense_rank ASC, distinct_course_level_in_student_id_enrolled_at_ ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_first_enrolled_at_date
    FROM rollup_base
)
-- Final projection
SELECT
    distinct_course_level_in_student_id_enrolled_at_,
    total_prev3_score_bucket_dense_rank,
    first_first_enrolled_at_date,
    days_since_prev_enrolled_at_date_grade_status_wi,
    second_first_first_enrolled_at_date
FROM sequenced_2
ORDER BY first_first_enrolled_at_date DESC, days_since_prev_enrolled_at_date_grade_status_wi, distinct_course_level_in_student_id_enrolled_at_
LIMIT 5
