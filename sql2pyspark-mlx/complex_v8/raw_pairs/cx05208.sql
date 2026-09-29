WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id <= 10
),
combined_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        c.dept AS course_dept,
        c.level AS course_level
    FROM filtered s
    LEFT JOIN courses c ON c.id = s.course_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_grade_status_course_id_by_en, 3) AS enrolled_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY course_dept ORDER BY enrolled_at ASC NULLS LAST, id), 3) AS enrolled_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_dept ORDER BY enrolled_at ASC NULLS LAST), 3) AS enrolled_at_percent_rank_2,
        NTILE(3) OVER (PARTITION BY course_dept ORDER BY enrolled_at ASC NULLS LAST, id) AS enrolled_at_tercile,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY student_id, course_dept ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_dept ORDER BY enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status, course_id ORDER BY enrolled_at ASC, id), 3) AS enrolled_at_cume_dist_2,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status, course_id ORDER BY enrolled_at ASC), 3) AS enrolled_at_cume_dist_3,
        NTILE(10) OVER (PARTITION BY course_dept ORDER BY enrolled_at ASC NULLS LAST, id) AS enrolled_at_decile
    FROM combined_details_course
    WINDOW w_grade_status_course_id_by_en AS (PARTITION BY grade_status, course_id ORDER BY enrolled_at ASC, id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(enrolled_at_tercile / NULLIF(enrolled_at_cume_dist_2, 0), 3) AS enrolled_at_tercile_per_enrolled_at_cume_dist_2,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM with_trends
)
SELECT
    id,
    student_id,
    course_id,
    enrolled_at,
    course_dept,
    course_level,
    enrolled_at_percent_rank,
    enrolled_at_cume_dist_3,
    enrolled_at_decile,
    enrolled_at_tercile_per_enrolled_at_cume_dist_2,
    CONCAT_WS(': ', term, course_level) AS term_course_level_label,
    LOWER(REGEXP_REPLACE(TRIM(course_level), ' ', '_')) AS course_level_slug
FROM derived
ORDER BY enrolled_at_percent_rank, enrolled_at_tercile_per_enrolled_at_cume_dist_2
