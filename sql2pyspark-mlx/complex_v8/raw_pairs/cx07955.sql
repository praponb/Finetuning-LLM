WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id >= 14
      AND course_id >= 2
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix,
        LENGTH(TRIM(term)) AS term_len,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
    FROM recent
),
without_matches AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.credits > 2
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY student_id ORDER BY term_len) AS term_len_rank,
        MAX(score) OVER (PARTITION BY grade_status, course_id) AS grade_status_course_id_max_score,
        MIN(term_len) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_term_len_so_far,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        ROUND(AVG(score) OVER (PARTITION BY student_id ORDER BY term_len, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS score_avg_3
    FROM without_matches
)
SELECT
    student_id,
    COALESCE(grade_status, 'ALL') AS grade_status,
    ROUND(STDDEV(term_len), 2) AS stddev_term_len
FROM metrics
GROUP BY ROLLUP(student_id, grade_status)
