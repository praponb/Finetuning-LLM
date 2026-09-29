WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.score) AS prior_score_12h
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count_2
    FROM in_window s
),
unique_rows AS (
    -- Keep the earliest row per student_id
    SELECT
        id,
        student_id,
        enrolled_at,
        prior_rows_12h,
        prior_score_12h,
        min_enrollment_score,
        enrollment_count,
        enrollment_count_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS recency_rank
        FROM summarized_rows
    ) ranked
    WHERE recency_rank = 1
),
student_id_stats AS (
    -- Avg enrollment_count_2 per student_id
    SELECT student_id, AVG(enrollment_count_2) AS avg_enrollment_count_2
    FROM unique_rows
    GROUP BY student_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_enrollment_count_2,
        ROUND(s.enrollment_count_2 - g.avg_enrollment_count_2, 2) AS enrollment_count_2_minus_avg
    FROM unique_rows s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.enrollment_count_2 > g.avg_enrollment_count_2
)
-- Final projection
SELECT
    id,
    student_id,
    enrolled_at,
    prior_rows_12h,
    prior_score_12h,
    min_enrollment_score,
    enrollment_count,
    enrollment_count_2,
    enrollment_count_2_minus_avg,
    COALESCE(min_enrollment_score, 0.0) AS min_enrollment_score_filled,
    CASE WHEN min_enrollment_score >= 65.0 THEN 'large' WHEN min_enrollment_score >= 30.0 THEN 'medium' ELSE 'small' END AS min_enrollment_score_tier
FROM compared
ORDER BY min_enrollment_score_filled DESC NULLS FIRST
