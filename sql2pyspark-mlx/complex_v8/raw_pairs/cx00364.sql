WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
    FROM enrollments
    WHERE term IS NOT NULL
      AND enrolled_at >= TIMESTAMP '2025-01-31 08:00:00'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY term, course_id ORDER BY enrolled_at DESC, id) AS enrolled_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY term, course_id ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term, course_id ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_percent_rank
    FROM scoped
),
enriched_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_label,
        s.enrolled_at_dense_rank,
        s.enrolled_at_cume_dist,
        s.enrolled_at_percent_rank,
        c.course_code
    FROM with_windows s
    JOIN courses c ON c.id = s.course_id
),
student_id_stats AS (
    -- Max enrolled_at_dense_rank per student_id
    SELECT student_id, MAX(enrolled_at_dense_rank) AS max_enrolled_at_dense_rank
    FROM enriched_course
    GROUP BY student_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_enrolled_at_dense_rank,
        ROUND(s.enrolled_at_dense_rank - g.max_enrolled_at_dense_rank, 2) AS enrolled_at_dense_rank_minus_max
    FROM enriched_course s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.enrolled_at_dense_rank = g.max_enrolled_at_dense_rank
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_at, grade_status, enrolled_at_dense_rank_minus_max
    FROM vs_group
) src
PIVOT (
    SUM(enrolled_at_dense_rank_minus_max)
    FOR grade_status IN ('WITHDRAWN', 'PASS', 'FAIL', 'INCOMPLETE')
)
ORDER BY enrolled_at
