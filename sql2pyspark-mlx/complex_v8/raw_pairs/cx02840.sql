WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        ABS(score - 34.2) AS score_distance
    FROM enrollments
    WHERE (TO_DATE(enrolled_at) < DATE '2025-02-24' OR score < 69.6)
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count,
        (SELECT COUNT(*) FILTER (WHERE e.term = '2024T2') FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_term_count
    FROM base s
),
enrollment_stats_2 AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at,
        e.last_enrollment_enrolled_at
    FROM with_stats s
    LEFT JOIN enrollment_stats_2 e ON e.student_id = s.student_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        term,
        score,
        grade_status,
        score_filled,
        score_distance,
        enrollment_count,
        enrollment_term_count,
        first_enrollment_enrolled_at,
        GREATEST(enrollment_count, score_filled) AS greatest_enrollment_count_score_filled
    FROM profiled
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(enrollment_term_count) OVER (PARTITION BY enrollment_count, term ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrollment_term_count,
        COUNT(*) OVER w_grade_status_enrollment_term AS rows_in_grade_status_enrollment_term_count
    FROM enriched
    WINDOW w_grade_status_enrollment_term AS (PARTITION BY grade_status, enrollment_term_count)
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        term,
        grade_status,
        score_filled,
        score_distance,
        enrollment_count,
        enrollment_term_count,
        COUNT(*) OVER (PARTITION BY grade_status, enrollment_count) AS rows_in_grade_status_enrollment_count,
        NTILE(4) OVER (PARTITION BY enrollment_count, last_enrollment_term_count ORDER BY first_enrollment_enrolled_at DESC, id) AS first_enrollment_enrolled_at_quartile,
        ROUND(AVG(score_filled) OVER (PARTITION BY enrollment_count, last_enrollment_term_count ORDER BY first_enrollment_enrolled_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS score_filled_avg_1_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY enrollment_count, last_enrollment_term_count ORDER BY first_enrollment_enrolled_at DESC), 3) AS first_enrollment_enrolled_at_cume_dist
    FROM with_trends
),
deduped AS (
    -- Keep the latest row per term
    SELECT
        id,
        term,
        grade_status,
        score_filled,
        score_distance,
        enrollment_count,
        enrollment_term_count,
        rows_in_grade_status_enrollment_count,
        first_enrollment_enrolled_at_quartile,
        score_filled_avg_1_prior,
        first_enrollment_enrolled_at_cume_dist
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term ORDER BY first_enrollment_enrolled_at_cume_dist DESC, id DESC) AS row_num
        FROM with_trends_2
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    term,
    grade_status,
    enrollment_term_count,
    first_enrollment_enrolled_at_quartile,
    score_filled_avg_1_prior,
    first_enrollment_enrolled_at_cume_dist,
    ROUND(score_filled / NULLIF(score_filled_avg_1_prior, 0), 3) AS score_filled_per_score_filled_avg_1_prior
FROM deduped
ORDER BY term DESC, enrollment_term_count DESC, id
LIMIT 5
