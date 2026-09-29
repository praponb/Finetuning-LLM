WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE course_id <= 3
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(score) AS min_score,
        SUM(score) AS total_score
    FROM cleaned
    WHERE score < 80.2
    GROUP BY course_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(min_score - 60.0) AS min_score_distance,
        GREATEST(distinct_ids, total_score) AS greatest_distinct_ids_total_score,
        ROUND(distinct_ids / NULLIF(total_score, 0), 3) AS distinct_ids_per_total_score
    FROM summary
    WHERE total_score > 150.0
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.course_id,
        s.distinct_ids,
        s.min_score,
        s.total_score,
        s.greatest_distinct_ids_total_score,
        s.distinct_ids_per_total_score,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status
    FROM scored s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        course_id,
        distinct_ids,
        min_score,
        total_score,
        greatest_distinct_ids_total_score,
        enrollment_id,
        enrollment_student_id,
        enrollment_enrolled_at,
        distinct_ids - LAG(distinct_ids) OVER (ORDER BY min_score DESC, course_id, enrollment_id) AS distinct_ids_change,
        DENSE_RANK() OVER (ORDER BY greatest_distinct_ids_total_score ASC, course_id, enrollment_id) AS greatest_distinct_ids_total_score_dense_rank,
        DENSE_RANK() OVER (ORDER BY min_score DESC) AS min_score_dense_rank,
        NTILE(5) OVER (ORDER BY min_score DESC, course_id, enrollment_id) AS min_score_quintile,
        ROUND(PERCENT_RANK() OVER (ORDER BY min_score DESC), 3) AS min_score_percent_rank,
        FIRST_VALUE(min_score) IGNORE NULLS OVER (ORDER BY greatest_distinct_ids_total_score ASC, course_id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_min_score
    FROM detail_rows
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        course_id,
        distinct_ids,
        greatest_distinct_ids_total_score,
        enrollment_id,
        enrollment_enrolled_at,
        distinct_ids_change,
        min_score_dense_rank,
        min_score_quintile,
        min_score_percent_rank,
        ROUND(total_score - AVG(total_score) OVER (PARTITION BY course_id), 2) AS total_score_vs_course_id_avg,
        SUM(min_score_quintile) OVER (PARTITION BY distinct_ids, course_id ORDER BY enrollment_enrolled_at ASC, course_id, enrollment_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS min_score_quintile_sum_2,
        first_known_min_score - LAG(first_known_min_score) OVER (PARTITION BY min_score_quintile ORDER BY enrollment_enrolled_at NULLS LAST, course_id, enrollment_id) AS first_known_min_score_change
    FROM ranked
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', enrollment_enrolled_at) AS enrollment_enrolled_at_quarter,
        TO_DATE(enrollment_enrolled_at) AS enrollment_enrolled_at_date
    FROM with_trends
)
-- Final projection
SELECT
    course_id,
    distinct_ids,
    greatest_distinct_ids_total_score,
    enrollment_enrolled_at,
    min_score_dense_rank,
    min_score_quintile,
    min_score_percent_rank,
    total_score_vs_course_id_avg,
    min_score_quintile_sum_2,
    enrollment_enrolled_at_date,
    COALESCE(min_score_dense_rank, 0) AS min_score_dense_rank_filled
FROM derived
ORDER BY min_score_dense_rank DESC
