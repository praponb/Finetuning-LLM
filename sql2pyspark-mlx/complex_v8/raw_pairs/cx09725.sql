WITH prepared_courses AS (
    -- Filter courses
    SELECT *
    FROM courses
    WHERE credits >= 2
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        course_code,
        credits,
        level,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown,
        LENGTH(TRIM(course_code)) AS course_code_len
    FROM prepared_courses
),
latest_per_key AS (
    -- Keep the earliest row per level, level_or_unknown
    SELECT
        id,
        course_code,
        credits,
        level,
        level_or_unknown,
        course_code_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY level, level_or_unknown ORDER BY course_code_len, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM latest_per_key s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
        )
      AND s.level_or_unknown IN ('UNKNOWN', 'core', 'intro')
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        course_code_len - credits AS course_code_len_minus_credits,
        CASE WHEN level_or_unknown IN ('intro', 'core') THEN 'group_a' WHEN level_or_unknown = 'UNKNOWN' THEN 'group_b' ELSE 'other' END AS level_or_unknown_group
    FROM qualified
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        MIN(score) AS min_enrollment_score,
        MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.min_enrollment_score,
        e.first_enrollment_enrolled_at
    FROM labeled s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(first_enrollment_enrolled_at, 2) OVER (PARTITION BY credits ORDER BY min_enrollment_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_enrollment_enrolled_at,
        SIZE(COLLECT_SET(level_or_unknown) OVER w_enrollment_count) AS distinct_level_or_unknown_in_enrollment_count,
        FIRST_VALUE(enrollment_count) OVER (PARTITION BY credits ORDER BY min_enrollment_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrollment_count,
        SIZE(COLLECT_SET(level) OVER w_enrollment_count) AS distinct_level_in_enrollment_count,
        MAX(min_enrollment_score) OVER w_enrollment_count AS enrollment_count_max_min_enrollment_score,
        MIN(credits) OVER (PARTITION BY enrollment_count) AS enrollment_count_min_credits
    FROM with_stats
    WINDOW w_enrollment_count AS (PARTITION BY enrollment_count)
)
SELECT
    distinct_level_in_enrollment_count,
    course_code_len,
    distinct_level_or_unknown_in_enrollment_count,
    GROUPING(distinct_level_in_enrollment_count) AS is_total_distinct_level_in_enrollment_count,
    ROUND(AVG(course_code_len), 2) AS avg_course_code_len,
    ARRAY_SORT(COLLECT_SET(course_code)) AS course_code_list,
    MIN(enrollment_count) AS min_enrollment_count
FROM sequenced
GROUP BY GROUPING SETS ((distinct_level_in_enrollment_count, course_code_len, distinct_level_or_unknown_in_enrollment_count), (distinct_level_in_enrollment_count))
ORDER BY distinct_level_in_enrollment_count NULLS LAST, course_code_len NULLS LAST, distinct_level_or_unknown_in_enrollment_count NULLS LAST
