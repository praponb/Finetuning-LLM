WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE score >= 34.2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        grade_status_len,
        term_or_unknown,
        FIRST_VALUE(score) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        ROUND(grade_status_len / NULLIF(SUM(grade_status_len) OVER w_term_or_unknown_student_id, 0), 4) AS grade_status_len_share_of_term_or_unknown_studen
    FROM filtered
    WINDOW w_term_or_unknown_student_id AS (PARTITION BY term_or_unknown, student_id)
)
SELECT id, metric_name, amount
FROM (
    SELECT id, first_score, grade_status_len_share_of_term_or_unknown_studen, score
    FROM with_trends
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric_name IN (first_score, grade_status_len_share_of_term_or_unknown_studen, score)
) unp
ORDER BY id, metric_name
