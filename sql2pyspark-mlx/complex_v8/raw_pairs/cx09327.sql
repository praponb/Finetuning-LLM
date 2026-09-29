WITH recent AS (
    -- Filter enrollments
    SELECT *
    FROM enrollments
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.total_enrollment_score
    FROM recent s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
latest AS (
    -- Keep the latest row per term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        total_enrollment_score
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term ORDER BY score DESC, id DESC) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS total_enrollment_score_2,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM latest s
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        enrollment_count,
        ROUND(100 * COUNT(CASE WHEN grade_status = 'INCOMPLETE' THEN 1 END) / COUNT(*), 1) AS pct_grade_status_incomplete,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list
    FROM summarized_rows
    GROUP BY enrollment_count
    HAVING AVG(total_enrollment_score) > 180.0
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE pct_grade_status_incomplete > (SELECT AVG(pct_grade_status_incomplete) FROM stats)
),
flattened AS (
    -- One row per term_list_item
    SELECT s.enrollment_count, s.pct_grade_status_incomplete, t.term_list_item
    FROM qualified s
    LATERAL VIEW EXPLODE(s.term_list) t AS term_list_item
)
-- Final projection
SELECT
    enrollment_count,
    pct_grade_status_incomplete,
    term_list_item,
    LOWER(REGEXP_REPLACE(TRIM(term_list_item), ' ', '_')) AS term_list_item_slug,
    LENGTH(TRIM(term_list_item)) AS term_list_item_len
FROM flattened
ORDER BY term_list_item_slug DESC
