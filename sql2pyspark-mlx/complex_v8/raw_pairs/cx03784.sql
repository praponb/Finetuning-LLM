WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 39.4) AS score_distance
    FROM enrollments
    WHERE term NOT IN ('2024T2', '2025T2')
      AND grade_status <> 'PASS'
),
with_activity AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND LENGTH(TRIM(st.student_name)) >= 8
              AND st.cohort = 'C2024'
        )
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) AS enrollment_count,
        ROUND(AVG(score), 2) AS avg_enrollment_score,
        COUNT(*) AS enrollment_count_2
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.avg_enrollment_score,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2
    FROM with_activity s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
unique_rows AS (
    -- Keep the earliest row per term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_distance,
        enrollment_count,
        avg_enrollment_score,
        enrollment_count_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term ORDER BY avg_enrollment_score, id) AS rn
        FROM with_counts
    ) ranked
    WHERE rn = 1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrollment_count,
        avg_enrollment_score,
        enrollment_count_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, enrollment_count ORDER BY enrollment_count DESC), 3) AS enrollment_count_percent_rank,
        ROUND(CUME_DIST() OVER w_student_id_enrollment_count, 3) AS enrollment_count_cume_dist,
        ROUND(AVG(enrollment_count_2) OVER (PARTITION BY student_id), 2) AS student_id_avg_enrollment_count_2,
        MAX(enrollment_count) OVER (PARTITION BY student_id, enrollment_count ORDER BY enrollment_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_enrollment_count_so_far,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id
    FROM unique_rows
    WINDOW w_student_id_enrollment_count AS (PARTITION BY student_id, enrollment_count ORDER BY enrollment_count DESC)
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    enrollment_count,
    avg_enrollment_score,
    enrollment_count_2,
    enrollment_count_percent_rank,
    enrollment_count_cume_dist,
    student_id_avg_enrollment_count_2,
    max_enrollment_count_so_far
FROM metrics
WHERE max_enrollment_count_so_far < 72
ORDER BY student_id_avg_enrollment_count_2 DESC
