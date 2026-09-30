WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix
    FROM enrollments
    WHERE (term NOT IN ('2024T1', '2025T1') OR score > 8.8)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        course_id,
        score,
        grade_status,
        LOWER(REGEXP_REPLACE(TRIM(term_prefix), ' ', '_')) AS term_prefix_slug,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM recent
    WHERE term_prefix IS NOT NULL
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', term_prefix_slug, grade_status) AS term_prefix_slug_grade_status_label,
        ROUND(score * 1.1, 2) AS score_adj
    FROM annotated
    WHERE term_prefix_slug IS NOT NULL
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at
    FROM tagged s
)
SELECT
    course_id,
    grade_status,
    enrollment_count,
    ROUND(SUM(score), 2) AS total_score,
    ROUND(SUM(enrollment_count) / NULLIF(SUM(score_adj), 0), 3) AS enrollment_count_to_score_adj_ratio,
    COUNT(DISTINCT course_id) AS distinct_course_ids
FROM with_counts
GROUP BY ROLLUP(course_id, grade_status, enrollment_count)
ORDER BY course_id NULLS LAST, grade_status NULLS LAST, enrollment_count NULLS LAST
