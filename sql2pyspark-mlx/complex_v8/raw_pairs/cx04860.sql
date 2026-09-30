WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        score,
        grade_status,
        enrolled_at,
        COUNT(*) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        COUNT(*) OVER w_grade_status AS rows_in_grade_status,
        LAST_VALUE(score) IGNORE NULLS OVER (ORDER BY score DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score
    FROM cleaned_enrollments
    WINDOW w_grade_status AS (PARTITION BY grade_status)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(last_known_score) OVER (ORDER BY running_count DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_last_known_score_so_far,
        DENSE_RANK() OVER (PARTITION BY student_id, grade_status ORDER BY running_count, id) AS running_count_dense_rank
    FROM windowed
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        student_id,
        score,
        grade_status,
        enrolled_at,
        running_count,
        rows_in_grade_status,
        last_known_score,
        max_last_known_score_so_far
    FROM with_trends
    WHERE running_count_dense_rank <= 2
      AND max_last_known_score_so_far >= 200.0
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at_2,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM top_per_group s
)
-- Final projection
SELECT
    id,
    student_id,
    score,
    grade_status,
    rows_in_grade_status,
    last_known_score,
    max_last_known_score_so_far,
    first_enrollment_enrolled_at_2,
    ROUND(last_known_score * 0.9, 2) AS last_known_score_adj,
    DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label
FROM with_counts
ORDER BY first_enrollment_enrolled_at_2 DESC
