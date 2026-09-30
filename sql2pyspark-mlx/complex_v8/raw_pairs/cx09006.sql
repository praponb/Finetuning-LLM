WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE score BETWEEN 8.8 AND 78.8
      AND grade_status = 'FAIL'
),
enriched AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.grade_status_len,
        s.term_or_unknown,
        c.level AS course_level,
        c.dept AS course_dept
    FROM prepared_enrollments s
    JOIN courses c ON c.id = s.course_id
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        course_dept,
        COUNT(*) AS n_rows,
        SUM(CASE WHEN course_level IN ('intro', 'core') THEN 1 ELSE 0 END) AS n_course_level_in_intro_core,
        ARRAY_SORT(COLLECT_SET(course_dept)) AS course_dept_list,
        COUNT(*) FILTER (WHERE course_level = 'advanced') AS n_course_level_advanced,
        COUNT(DISTINCT course_level) AS distinct_course_levels,
        SUM(grade_status_len) AS total_grade_status_len
    FROM enriched
    GROUP BY course_dept
    HAVING COUNT(*) >= 3
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(course_dept, 'UNKNOWN') AS course_dept_or_unknown,
        ABS(n_rows - 7) AS n_rows_distance,
        ABS(n_rows - 6) AS n_rows_distance_2
    FROM rollup_base
    WHERE distinct_course_levels BETWEEN 1 AND 7
),
scored_2 AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN course_dept IN ('Art', 'Math') THEN 'group_a' WHEN course_dept = 'Bio' THEN 'group_b' ELSE 'other' END AS course_dept_group,
        COALESCE(total_grade_status_len, 0) AS total_grade_status_len_filled,
        SUBSTRING(TRIM(course_dept_or_unknown), 1, 4) AS course_dept_or_unknown_prefix
    FROM scored
    WHERE course_dept_or_unknown IN ('Bio', 'Math', 'CS')
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored_2
    WHERE total_grade_status_len_filled > (SELECT AVG(total_grade_status_len_filled) FROM scored_2)
),
tagged AS (
    -- Derive helper columns
    SELECT
        course_dept,
        n_rows,
        n_course_level_in_intro_core,
        course_dept_list,
        n_course_level_advanced,
        distinct_course_levels,
        total_grade_status_len,
        course_dept_or_unknown,
        n_rows_distance_2,
        course_dept_group,
        course_dept_or_unknown_prefix,
        SUBSTRING(TRIM(course_dept), 1, 2) AS course_dept_prefix,
        ARRAY_JOIN(ARRAY_SORT(course_dept_list), ',') AS course_dept_list_text,
        SUBSTRING(TRIM(course_dept_or_unknown), 1, 4) AS course_dept_or_unknown_prefix_2
    FROM without_matches
    WHERE course_dept_or_unknown_prefix IS NOT NULL
)
-- Final projection
SELECT
    course_dept,
    n_rows,
    course_dept_list,
    total_grade_status_len,
    course_dept_group,
    course_dept_or_unknown_prefix,
    course_dept_list_text,
    CONCAT_WS(' / ', course_dept_or_unknown, course_dept_list_text) AS course_dept_or_unknown_course_dept_list_text_lab,
    LENGTH(TRIM(course_dept_list_text)) AS course_dept_list_text_len
FROM tagged
WHERE distinct_course_levels >= 2
