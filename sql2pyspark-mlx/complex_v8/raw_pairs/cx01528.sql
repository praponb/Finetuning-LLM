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
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE score < 85.8
      AND enrolled_at < TIMESTAMP '2025-02-18 08:00:00'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY score_filled DESC) AS score_filled_dense_rank,
        NTILE(3) OVER (PARTITION BY term ORDER BY score, id) AS score_tercile,
        ROUND(PERCENT_RANK() OVER (ORDER BY score_filled DESC, id), 3) AS score_filled_percent_rank
    FROM valid
),
with_parent_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_year,
        s.score_filled,
        s.score_tercile,
        s.score_filled_percent_rank,
        st.major AS student_major,
        st.is_international AS student_is_international,
        st.student_name
    FROM ranked s
    JOIN students st ON st.id = s.student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS total_enrollment_score
    FROM with_parent_student s
)
-- Subtotals with GROUPING SETS
SELECT
    student_id,
    student_is_international,
    COUNT(DISTINCT student_major) AS distinct_student_majors,
    COUNT(*) AS cnt,
    COUNT(DISTINCT course_id) AS distinct_course_ids
FROM profiled
GROUP BY GROUPING SETS ((student_id, student_is_international), (student_id))
ORDER BY student_id NULLS LAST, student_is_international NULLS LAST
