WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE term IS NOT NULL
),
tagged AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(term)) AS term_len
    FROM cleaned_enrollments
),
enriched_student AS (
    -- Attach student attributes
    SELECT
        s.*,
        st.student_name,
        st.cohort AS student_cohort,
        c.course_code
    FROM tagged s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        c.credits AS course_credits,
        c.level AS course_level,
        c.dept AS course_dept
    FROM enriched_student s
    JOIN courses c ON c.id = s.course_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        term,
        score,
        score_filled,
        term_len,
        student_name,
        student_cohort,
        course_code,
        course_credits,
        course_level,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term, course_level ORDER BY score_filled, id), 3) AS score_filled_percent_rank,
        SIZE(COLLECT_SET(course_dept) OVER (PARTITION BY course_level)) AS distinct_course_dept_in_course_level,
        COUNT(*) OVER w_course_level AS rows_in_course_level,
        DENSE_RANK() OVER (PARTITION BY term, course_level ORDER BY score_filled) AS score_filled_dense_rank
    FROM enriched
    WINDOW w_course_level AS (PARTITION BY course_level)
)
SELECT
    id,
    term,
    score,
    score_filled,
    term_len,
    student_cohort,
    course_credits,
    course_level,
    score_filled_percent_rank,
    distinct_course_dept_in_course_level,
    rows_in_course_level,
    score_filled_dense_rank,
    COALESCE(score, 0.0) AS score_filled_2,
    CASE WHEN course_level IN ('intro', 'core') THEN 'group_a' WHEN course_level = 'advanced' THEN 'group_b' ELSE 'other' END AS course_level_group
FROM ranked
ORDER BY student_cohort NULLS LAST, course_level DESC
