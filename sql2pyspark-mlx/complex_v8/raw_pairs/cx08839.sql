WITH cleaned_courses AS (
    -- Filter courses
    SELECT
        id,
        dept,
        credits,
        level
    FROM courses
    WHERE level NOT IN ('intro', 'advanced')
      AND dept IN ('Math', 'CS', 'Art')
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        dept,
        credits,
        level,
        LOWER(TRIM(dept)) AS dept_clean
    FROM cleaned_courses
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        dept,
        level,
        ROW_NUMBER() OVER w_level_dept_by_credits AS rn,
        ROW_NUMBER() OVER w_level_by_credits AS rn_2,
        FIRST_VALUE(credits) IGNORE NULLS OVER (PARTITION BY level ORDER BY credits ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credits,
        COUNT(*) OVER (PARTITION BY dept, level) AS rows_in_dept_level
    FROM annotated
    WINDOW w_level_dept_by_credits AS (PARTITION BY level, dept ORDER BY credits DESC, id), w_level_by_credits AS (PARTITION BY level ORDER BY credits ASC NULLS LAST, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        rn,
        rn_2,
        rows_in_dept_level,
        ABS(rn_2 - 6) AS rn_2_distance
    FROM metrics
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        rn,
        rows_in_dept_level,
        rn_2_distance
    FROM scored
    WHERE rn_2 <= 2
      AND rows_in_dept_level >= 4
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.course_id = s.id) AS max_enrollment_score
    FROM top_per_group s
)
SELECT id, rows_in_dept_level, max_enrollment_score
FROM summarized_rows
ORDER BY max_enrollment_score DESC, id DESC
