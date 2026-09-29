WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        CASE WHEN grade_status IN ('FAIL', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'PASS' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE (student_id IS NOT NULL OR grade_status NOT IN ('FAIL', 'PASS'))
),
with_parent_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.days_since_enrolled_at,
        s.grade_status_group,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort,
        c.course_code,
        c.level AS course_level
    FROM base_enrollments s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
    WHERE st.enrolled_on >= DATE '2024-05-18'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status_group,
        student_enrolled_on,
        student_cohort,
        course_code,
        ROW_NUMBER() OVER w_course_id_student_id_by_enro AS rn,
        FIRST_VALUE(score) OVER (PARTITION BY term, course_level ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        ROUND(days_since_enrolled_at - AVG(days_since_enrolled_at) OVER (PARTITION BY DATE_TRUNC('MONTH', student_enrolled_on), student_cohort), 2) AS days_since_enrolled_at_vs_student_enrolled_on_mo,
        ROW_NUMBER() OVER (PARTITION BY grade_status_group ORDER BY enrolled_at DESC NULLS FIRST, id) AS enrolled_at_seq,
        NTILE(3) OVER w_grade_status_group_by_enroll AS enrolled_at_tercile,
        ROUND(AVG(days_since_enrolled_at) OVER (PARTITION BY grade_status_group ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS days_since_enrolled_at_avg_4,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_course_id_student_id_by_enro)) / 60, 1) AS minutes_since_prev_enrolled_at,
        LAST_VALUE(days_since_enrolled_at) OVER (PARTITION BY grade_status_group ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_enrolled_at,
        FIRST_VALUE(days_since_enrolled_at) IGNORE NULLS OVER (PARTITION BY term, course_level ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_enrolled_at,
        SIZE(COLLECT_SET(term) OVER w_student_enrolled_on_month_st) AS distinct_term_in_student_enrolled_on_month_stude
    FROM with_parent_student
    WINDOW w_course_id_student_id_by_enro AS (PARTITION BY course_id, student_id ORDER BY enrolled_at DESC, id), w_grade_status_group_by_enroll AS (PARTITION BY grade_status_group ORDER BY enrolled_at DESC NULLS FIRST, id), w_student_enrolled_on_month_st AS (PARTITION BY DATE_TRUNC('MONTH', student_enrolled_on), student_cohort)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        student_enrolled_on,
        student_cohort,
        course_code,
        rn,
        first_score,
        days_since_enrolled_at_vs_student_enrolled_on_mo,
        enrolled_at_tercile,
        days_since_enrolled_at_avg_4,
        minutes_since_prev_enrolled_at,
        last_days_since_enrolled_at,
        distinct_term_in_student_enrolled_on_month_stude,
        LAST_VALUE(student_enrolled_on) OVER (PARTITION BY rn, student_cohort ORDER BY first_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_student_enrolled_on,
        SUM(first_score) OVER (PARTITION BY grade_status_group, rn) AS grade_status_group_rn_sum_first_score,
        NTH_VALUE(enrolled_at_tercile, 2) OVER (PARTITION BY course_id, distinct_term_in_student_enrolled_on_month_stude ORDER BY last_days_since_enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_tercile,
        LAST_VALUE(days_since_enrolled_at_avg_4) OVER (PARTITION BY rn, student_cohort ORDER BY first_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_enrolled_at_avg_4,
        ROUND(enrolled_at_tercile / NULLIF(SUM(enrolled_at_tercile) OVER (PARTITION BY grade_status_group, rn), 0), 4) AS enrolled_at_tercile_share_of_grade_status_group_
    FROM sequenced
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    student_enrolled_on,
    student_cohort,
    rn,
    first_score,
    days_since_enrolled_at_avg_4,
    minutes_since_prev_enrolled_at,
    last_days_since_enrolled_at,
    distinct_term_in_student_enrolled_on_month_stude,
    last_student_enrolled_on,
    grade_status_group_rn_sum_first_score,
    enrolled_at_tercile_share_of_grade_status_group_,
    SUBSTRING(TRIM(course_code), 1, 4) AS course_code_prefix,
    CONCAT(student_cohort, '#', CAST(enrolled_at_tercile AS STRING)) AS student_cohort_ref
FROM with_trends
ORDER BY rn DESC, course_code_prefix, id
LIMIT 5
