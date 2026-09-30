WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE course_id IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at,
        grade_status_or_unknown,
        term_slug,
        score_filled,
        DENSE_RANK() OVER w_student_id_grade_status_or_u AS score_dense_rank,
        ROW_NUMBER() OVER w_student_id_grade_status_or_u AS score_seq,
        RANK() OVER w_student_id_grade_status_or_u AS score_rank,
        SUM(score) OVER (PARTITION BY term) AS term_sum_score
    FROM cleaned_enrollments
    WINDOW w_student_id_grade_status_or_u AS (PARTITION BY student_id, grade_status_or_unknown ORDER BY score DESC, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        enrolled_at,
        grade_status_or_unknown,
        term_slug,
        score_filled,
        score_dense_rank,
        score_seq,
        term_sum_score,
        RANK() OVER (PARTITION BY score_dense_rank, score_seq ORDER BY score_rank) AS score_rank_rank,
        ROUND(AVG(score_seq) OVER (PARTITION BY course_id ORDER BY score_filled DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_seq_avg_7
    FROM with_trends
),
tagged AS (
    -- Add calculated columns
    SELECT *, COALESCE(score_filled, 0.0) AS score_filled_filled
    FROM with_windows
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    enrolled_at,
    grade_status_or_unknown,
    term_slug,
    score_filled,
    term_sum_score,
    score_rank_rank,
    score_filled_filled
FROM with_activity
ORDER BY term_sum_score NULLS LAST, score_filled_filled DESC
