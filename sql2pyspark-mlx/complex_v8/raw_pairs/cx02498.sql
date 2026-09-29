WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status IS NOT NULL
      AND student_id >= 9
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT student_id, SUM(score) AS total_score, COUNT(*) AS cnt
    FROM recent_enrollments
    GROUP BY 1
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS total_enrollment_score,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM rollup_base s
)
SELECT
    student_id,
    cnt,
    total_enrollment_score,
    avg_enrollment_score,
    enrollment_count,
    ABS(total_score - 330.0) AS total_score_distance
FROM with_stats
