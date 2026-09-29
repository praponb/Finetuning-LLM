WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        ABS(score - 69.6) AS score_distance
    FROM enrollments
    WHERE score > 43.5
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(score_filled) OVER (PARTITION BY grade_status, DATE_TRUNC('MONTH', enrolled_at)) AS grade_status_enrolled_at_month_min_score_filled,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER (PARTITION BY grade_status, DATE_TRUNC('MONTH', enrolled_at)), 0), 4) AS score_distance_share_of_grade_status_enrolled_at,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist,
        COUNT(*) OVER (PARTITION BY MONTH(enrolled_at)) AS rows_in_enrolled_at_month,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY score_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        MAX(score) OVER (ORDER BY score_filled DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_max_2_prior,
        LAST_VALUE(enrolled_at) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        ROUND(CUME_DIST() OVER (ORDER BY score_filled DESC, id), 3) AS score_filled_cume_dist,
        SUM(score) OVER (ORDER BY score_distance, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_sum_6_prior,
        ROW_NUMBER() OVER (ORDER BY enrolled_at, id) AS enrolled_at_seq,
        COUNT(*) OVER (PARTITION BY grade_status, DATE_TRUNC('MONTH', enrolled_at)) AS rows_in_grade_status_enrolled_at_month
    FROM valid
),
grouped AS (
    -- Group-level totals
    SELECT
        student_id,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        MAX(enrolled_at) AS last_enrolled_at_2,
        PERCENTILE_APPROX(score_distance_share_of_grade_status_enrolled_at, 0.5) AS median_score_distance_share_of_grade_status_enro,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(score_distance, 0.5) AS median_score_distance
    FROM ranked
    WHERE score_distance < 75.0
    GROUP BY student_id
    HAVING COUNT(*) >= 2
)
SELECT
    student_id,
    distinct_student_ids,
    last_enrolled_at_2,
    median_score_distance_share_of_grade_status_enro,
    ABS(median_score_distance - 55.0) AS median_score_distance_distance,
    ROUND(median_score_distance * 1.1, 2) AS median_score_distance_adj
FROM grouped
ORDER BY median_score_distance_share_of_grade_status_enro
