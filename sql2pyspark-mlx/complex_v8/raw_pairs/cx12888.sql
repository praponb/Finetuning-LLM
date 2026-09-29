WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len,
        FLOOR(score / 25) * 25 AS score_bucket,
        UPPER(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE course_id >= 3
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS min_enrollment_score,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS total_enrollment_score
    FROM scoped s
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        INITCAP(TRIM(grade_status)) AS grade_status_clean,
        SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix,
        GREATEST(min_enrollment_score, score) AS greatest_min_enrollment_score_score
    FROM profiled
),
enriched_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.score_bucket,
        s.term_clean,
        s.min_enrollment_score,
        s.grade_status_clean,
        s.grade_status_prefix,
        s.greatest_min_enrollment_score_score,
        c.level AS course_level,
        c.course_code,
        c.dept AS course_dept
    FROM enriched s
    JOIN courses c ON c.id = s.course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS total_enrollment_score
    FROM enriched_2 s
),
enriched_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.score_bucket,
        s.term_clean,
        s.grade_status_prefix,
        s.greatest_min_enrollment_score_score,
        s.course_level,
        s.course_code,
        s.course_dept,
        s.total_enrollment_score,
        c.course_code AS course_course_code,
        c.dept AS course_dept_2,
        st.is_international AS student_is_international
    FROM summarized_rows s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
enrollment_stats_3 AS (
    -- Per-student enrollments metrics
    SELECT student_id, MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at
    FROM enriched_course s
    LEFT JOIN enrollment_stats_3 e ON e.student_id = s.student_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, greatest_min_enrollment_score_score, score
    FROM with_counts
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (greatest_min_enrollment_score_score, score)
) unp
ORDER BY id, metric_name
