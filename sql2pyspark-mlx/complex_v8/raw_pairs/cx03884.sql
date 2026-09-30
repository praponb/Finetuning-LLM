WITH valid AS (
    -- Filter courses
    SELECT
        id,
        dept,
        credits,
        level,
        COALESCE(credits, 0) AS credits_filled
    FROM courses
    WHERE dept = 'CS'
      AND LENGTH(TRIM(course_code)) >= 8
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.student_id <= 11
              AND e.grade_status IS NOT NULL
        )
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        ROUND(AVG(score), 2) AS avg_enrollment_score,
        ROUND(AVG(score), 2) AS avg_enrollment_score_2
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.avg_enrollment_score,
        e.avg_enrollment_score_2
    FROM screened s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.credits,
        s.level,
        s.avg_enrollment_score,
        s.avg_enrollment_score_2,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM with_counts s
    LEFT JOIN enrollments e ON e.course_id = s.id AND e.enrolled_at < TIMESTAMP '2025-03-08 11:00:00'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY enrollment_enrolled_at ASC, id, enrollment_id) AS enrollment_enrolled_at_rank,
        NTH_VALUE(avg_enrollment_score_2, 2) OVER (PARTITION BY level, enrollment_id ORDER BY credits DESC NULLS FIRST, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_avg_enrollment_score_2,
        ROUND(AVG(credits) OVER w_enrollment_id_level, 2) AS enrollment_id_level_avg_credits,
        SUM(avg_enrollment_score_2) OVER (PARTITION BY enrollment_id, level) AS enrollment_id_level_sum_avg_enrollment_score_2,
        ROUND(AVG(avg_enrollment_score_2) OVER (ORDER BY enrollment_enrolled_at ASC, id, enrollment_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS avg_enrollment_score_2_avg_3_prior
    FROM exploded_detail_enrollments
    WINDOW w_enrollment_id_level AS (PARTITION BY enrollment_id, level)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        credits,
        level,
        avg_enrollment_score_2,
        enrollment_id,
        enrollment_student_id,
        enrollment_enrolled_at,
        enrollment_enrolled_at_rank,
        enrollment_id_level_avg_credits,
        avg_enrollment_score_2_avg_3_prior,
        ABS(avg_enrollment_score - 40.0) AS avg_enrollment_score_distance
    FROM sequenced
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM enriched
    WHERE enrollment_enrolled_at_rank = 1
)
SELECT id, enrollment_id, metric_name, amount
FROM (
    SELECT id, enrollment_id, enrollment_enrolled_at_rank, credits
    FROM first_rows
) src
UNPIVOT (
    amount FOR metric_name IN (enrollment_enrolled_at_rank, credits)
) unp
