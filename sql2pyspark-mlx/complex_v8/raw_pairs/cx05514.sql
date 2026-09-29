WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        INITCAP(TRIM(term)) AS term_clean,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE course_id <= 10
),
with_parent_course AS (
    -- Join courses
    SELECT s.*, c.dept AS course_dept
    FROM base_enrollments s
    JOIN courses c ON c.id = s.course_id AND c.level = 'advanced'
),
with_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.score_filled,
        s.score_bucket,
        s.course_dept,
        c.level AS course_level,
        st.is_international AS student_is_international
    FROM with_parent_course s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
deduped AS (
    -- Keep the earliest row per term
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        score_filled,
        score_bucket,
        course_dept,
        course_level,
        student_is_international
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term ORDER BY score_bucket, id) AS recency_rank
        FROM with_details_course
    ) ranked
    WHERE recency_rank = 1
),
rollup_base AS (
    -- Summarise per group
    SELECT
        term,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        SUM(score_bucket) AS total_score_bucket,
        MIN(score_bucket) AS min_score_bucket,
        COUNT(*) AS cnt,
        COUNT(DISTINCT grade_status) AS distinct_grade_status
    FROM deduped
    GROUP BY 1
    HAVING COUNT(*) >= 2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(distinct_grade_status, 2) OVER (PARTITION BY term, distinct_grade_status ORDER BY distinct_grade_status DESC, term ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_grade_status,
        MIN(distinct_grade_status) OVER (PARTITION BY distinct_grade_status) AS distinct_grade_status_min_distinct_grade_status,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY distinct_grade_status)) AS distinct_term_in_distinct_grade_status,
        FIRST_VALUE(distinct_grade_status) OVER (PARTITION BY term, distinct_grade_status ORDER BY distinct_grade_status DESC, term ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_grade_status
    FROM rollup_base
)
-- Final projection
SELECT
    term,
    term_list,
    total_score_bucket,
    cnt,
    distinct_grade_status,
    distinct_grade_status_min_distinct_grade_status,
    distinct_term_in_distinct_grade_status,
    first_distinct_grade_status
FROM ranked
ORDER BY distinct_term_in_distinct_grade_status, cnt DESC
