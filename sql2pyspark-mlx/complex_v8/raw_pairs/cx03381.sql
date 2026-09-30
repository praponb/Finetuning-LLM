WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status = 'INCOMPLETE'
      AND score > 13.9
),
combined_details_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        c.credits AS course_credits
    FROM base s
    JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY student_id, course_id ORDER BY course_credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER (PARTITION BY course_credits), 0), 4) AS course_credits_share_of_course_credits,
        LEAD(course_credits, 1, 0) OVER w_all_by_course_credits AS next_course_credits,
        course_credits - LAG(course_credits) OVER w_all_by_enrolled_at AS course_credits_change,
        NTILE(3) OVER (ORDER BY course_credits DESC, id) AS course_credits_tercile,
        RANK() OVER w_student_id_course_id_by_cour_2 AS course_credits_rank,
        NTH_VALUE(course_credits, 2) OVER (ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_course_credits,
        ROUND(AVG(course_credits) OVER (PARTITION BY student_id, term), 2) AS student_id_term_avg_course_credits
    FROM combined_details_course
    WINDOW w_all_by_course_credits AS (ORDER BY course_credits DESC, id), w_all_by_enrolled_at AS (ORDER BY enrolled_at, id), w_student_id_course_id_by_cour_2 AS (PARTITION BY student_id, course_id ORDER BY course_credits, id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY term, course_credits_change)) AS distinct_course_id_in_term_course_credits_change,
        SIZE(COLLECT_SET(grade_status) OVER w_term_course_credits_change) AS distinct_grade_status_in_term_course_credits_cha,
        LAST_VALUE(course_credits) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_enrolled_at) ORDER BY student_id_term_avg_course_credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_course_credits,
        ROUND(AVG(course_credits) OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_enrolled_at) ORDER BY student_id_term_avg_course_credits, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS course_credits_avg_3_prior,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_enrolled_at) ORDER BY student_id_term_avg_course_credits, id) AS student_id_term_avg_course_credits_tercile,
        MAX(course_credits_tercile) OVER (PARTITION BY TO_DATE(enrolled_at), DATE_TRUNC('MONTH', first_known_enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS course_credits_tercile_max_4
    FROM sequenced
    WINDOW w_term_course_credits_change AS (PARTITION BY term, course_credits_change)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    enrolled_at,
    first_known_enrolled_at,
    next_course_credits,
    course_credits_change,
    course_credits_tercile,
    course_credits_rank,
    second_course_credits,
    student_id_term_avg_course_credits,
    last_known_course_credits,
    course_credits_avg_3_prior,
    student_id_term_avg_course_credits_tercile,
    course_credits_tercile_max_4,
    CASE WHEN grade_status IN ('PASS', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group,
    DATE_FORMAT(first_known_enrolled_at, 'yyyy-MM-dd') AS first_known_enrolled_at_label
FROM metrics
WHERE enrolled_at < TIMESTAMP '2025-02-24 12:00:00'
ORDER BY enrolled_at DESC, id NULLS LAST
