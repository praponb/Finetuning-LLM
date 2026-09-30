WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day,
        CASE WHEN score >= 78.8 THEN 'A' WHEN score >= 38.3 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE (enrolled_at < TIMESTAMP '2025-02-18 11:00:00' OR term = '2025T2')
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.enrolled_at_day,
        s.score_tier,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term
    FROM base_enrollments s
    JOIN enrollments e ON e.course_id = s.course_id
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM detail_rows_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status = 'FAIL'
        )
      AND TO_DATE(s.enrollment_enrolled_at) < DATE '2025-02-06'
),
combined_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.score_tier,
        s.enrollment_id,
        s.enrollment_score,
        s.enrollment_grade_status,
        s.enrollment_enrolled_at,
        s.enrollment_term,
        st.major AS student_major
    FROM screened s
    JOIN students st ON st.id = s.student_id
),
screened_2 AS (
    -- Keep rows whose enrollment matches
    SELECT s.*
    FROM combined_details_student s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.id = s.enrollment_id
              AND e.term = '2024T2'
              AND e.enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(score_tier) OVER (PARTITION BY student_id)) AS distinct_score_tier_in_student_id,
        ROUND(AVG(enrollment_score) OVER (PARTITION BY student_major, enrollment_grade_status ORDER BY score ASC, id, enrollment_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS enrollment_score_avg_6_prior,
        score - LAG(score) OVER w_enrollment_term_term_by_enro AS score_change,
        COUNT(*) OVER (PARTITION BY enrollment_term, term ORDER BY enrolled_at NULLS LAST, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(score - AVG(score) OVER w_student_id, 2) AS score_vs_student_id_avg
    FROM screened_2
    WINDOW w_enrollment_term_term_by_enro AS (PARTITION BY enrollment_term, term ORDER BY enrolled_at NULLS LAST, id, enrollment_id), w_student_id AS (PARTITION BY student_id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, enrollment_id, field, amount
FROM (
    SELECT id, enrollment_id, running_count, distinct_score_tier_in_student_id
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    amount FOR field IN (running_count, distinct_score_tier_in_student_id)
) unp
ORDER BY id, enrollment_id, field
