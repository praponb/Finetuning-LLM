WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND grade_status IN ('FAIL', 'WITHDRAWN', 'PASS')
),
with_details AS (
    -- Join courses
    SELECT
        s.*,
        c.credits AS course_credits,
        c.dept AS course_dept,
        c.level AS course_level
    FROM recent s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept = 'Math'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_student_id_grade_status_by_c, 3) AS course_credits_percent_rank,
        MAX(course_credits) OVER (PARTITION BY grade_status, course_dept) AS grade_status_course_dept_max_course_credits,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, grade_status ORDER BY course_credits DESC), 3) AS course_credits_percent_rank_2,
        LEAD(course_credits) OVER (PARTITION BY course_level, grade_status ORDER BY score, id) AS next_course_credits,
        score - LAG(score) OVER w_all_by_enrolled_at AS score_change,
        LEAD(enrolled_at) OVER (PARTITION BY course_level, grade_status ORDER BY score, id) AS next_enrolled_at,
        ROUND(AVG(course_credits) OVER w_course_id, 2) AS course_id_avg_course_credits,
        LEAD(enrolled_at, 2) OVER w_student_id_grade_status_by_c AS next2_enrolled_at,
        NTILE(3) OVER (ORDER BY enrolled_at DESC, id) AS enrolled_at_tercile,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_level, grade_status ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at
    FROM with_details
    WINDOW w_student_id_grade_status_by_c AS (PARTITION BY student_id, grade_status ORDER BY course_credits DESC, id), w_all_by_enrolled_at AS (ORDER BY enrolled_at DESC, id), w_course_id AS (PARTITION BY course_id)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at_label,
        course_credits,
        course_dept,
        course_level,
        course_credits_percent_rank_2,
        next_course_credits,
        score_change,
        next_enrolled_at,
        next2_enrolled_at,
        enrolled_at_tercile,
        first_known_enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        DATEDIFF(DATE '2025-06-30', next2_enrolled_at) AS days_since_next2_enrolled_at,
        CASE WHEN course_dept IN ('Biz', 'Art') THEN 'group_a' WHEN course_dept = 'Bio' THEN 'group_b' ELSE 'other' END AS course_dept_group
    FROM with_windows
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    grade_status,
    enrolled_at,
    enrolled_at_label,
    course_credits,
    course_level,
    score_change,
    next_enrolled_at,
    next2_enrolled_at,
    enrolled_at_tercile,
    first_known_enrolled_at,
    grade_status_slug,
    course_dept_group,
    CASE WHEN course_level IN ('advanced', 'intro') THEN 'group_a' WHEN course_level = 'core' THEN 'group_b' ELSE 'other' END AS course_level_group
FROM labeled
ORDER BY course_dept_group NULLS LAST
