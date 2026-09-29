WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        CONCAT_WS(' / ', term, grade_status) AS term_grade_status_label,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE grade_status <> 'PASS'
      AND student_id IS NOT NULL
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(score) AS min_enrollment_score,
        ROUND(AVG(score), 2) AS avg_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        e.avg_enrollment_score
    FROM filtered_enrollments s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        enrolled_at,
        score_filled,
        term_grade_status_label,
        grade_status_slug,
        avg_enrollment_score,
        LEAD(score_filled) OVER (ORDER BY enrolled_at DESC, id) AS next_score_filled,
        LAG(score_filled) OVER (ORDER BY enrolled_at DESC, id) AS prev_score_filled,
        LAST_VALUE(score_filled) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_filled,
        MAX(min_enrollment_score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS min_enrollment_score_max_1_prior,
        FIRST_VALUE(score) IGNORE NULLS OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score
    FROM summarized_rows
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id IS NOT NULL
        )
),
with_activity_2 AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id <= 11
              AND e.term = '2024T1'
        )
      AND s.term IN ('2025T2', '2024T1', '2025T1')
),
latest AS (
    -- Keep the latest row per term, student_id
    SELECT
        id,
        student_id,
        term,
        enrolled_at,
        score_filled,
        term_grade_status_label,
        grade_status_slug,
        avg_enrollment_score,
        next_score_filled,
        prev_score_filled,
        last_score_filled,
        min_enrollment_score_max_1_prior,
        first_known_score
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term, student_id ORDER BY last_score_filled DESC, id DESC) AS row_num
        FROM with_activity_2
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    student_id,
    term,
    enrolled_at,
    score_filled,
    term_grade_status_label,
    grade_status_slug,
    avg_enrollment_score,
    next_score_filled,
    prev_score_filled,
    last_score_filled,
    min_enrollment_score_max_1_prior
FROM latest
ORDER BY min_enrollment_score_max_1_prior DESC
