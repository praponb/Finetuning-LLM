WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        c.level AS course_level
    FROM recent_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
),
totals AS (
    -- Group-level totals
    SELECT
        student_id,
        course_level,
        course_id,
        COUNT(*) AS cnt,
        SUM(CASE WHEN course_level IN ('advanced', 'core') THEN 1 ELSE 0 END) AS n_course_level_in_advanced_cor,
        ARRAY_SORT(COLLECT_SET(course_level)) AS course_level_list,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        ROUND(100 * COUNT(CASE WHEN course_level IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_course_level_is_not_null
    FROM joined
    GROUP BY student_id, course_level, course_id
    HAVING COUNT(*) >= 2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(pct_course_level_is_not_null) OVER (PARTITION BY student_id, course_level ORDER BY pct_course_level_is_not_null, student_id, course_level, course_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_pct_course_level_is_not_null,
        ROUND(cnt / NULLIF(SUM(cnt) OVER (PARTITION BY student_id, cnt), 0), 4) AS cnt_share_of_student_id_cnt
    FROM totals
)
-- Final projection
SELECT
    student_id,
    course_level,
    course_id,
    cnt,
    n_course_level_in_advanced_cor,
    course_level_list,
    distinct_course_ids,
    last_pct_course_level_is_not_null,
    cnt_share_of_student_id_cnt,
    LEAST(cnt, distinct_course_ids) AS least_cnt_distinct_course_ids,
    ABS(distinct_course_ids - 4) AS distinct_course_ids_distance
FROM with_trends
ORDER BY distinct_course_ids DESC
