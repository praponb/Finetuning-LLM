WITH joined_ranges AS (
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
enriched AS (
    -- Label and score rows
    SELECT *, ROUND(prior_score_12h * 0.9, 2) AS prior_score_12h_adj
    FROM joined_ranges
    WHERE enrolled_at < TIMESTAMP '2025-03-08 09:00:00'
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS max_enrollment_score,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score,
        (SELECT COUNT(CASE WHEN e.grade_status NOT IN ('INCOMPLETE', 'WITHDRAWN') THEN 1 END) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count
    FROM enriched s
)
-- Subtotals with ROLLUP
SELECT
    student_id,
    enrollment_grade_status_count,
    GROUPING(student_id) AS is_total_student_id,
    PERCENTILE_APPROX(max_enrollment_score, 0.5) AS median_max_enrollment_score
FROM profiled
GROUP BY ROLLUP(student_id, enrollment_grade_status_count)
ORDER BY student_id NULLS LAST, enrollment_grade_status_count NULLS LAST
