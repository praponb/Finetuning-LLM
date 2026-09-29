WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE student_id <= 13
      AND TO_DATE(enrolled_at) < DATE '2025-03-08'
),
grade_status_stats AS (
    -- Max score per grade_status
    SELECT grade_status, MAX(score) AS max_score
    FROM filtered
    GROUP BY grade_status
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_score, ROUND(s.score - g.max_score, 2) AS score_minus_max
    FROM filtered s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score = g.max_score
),
with_parent AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.max_score,
        s.score_minus_max,
        c.level AS course_level,
        c.credits AS course_credits,
        c.dept AS course_dept,
        st.student_name,
        st.cohort AS student_cohort
    FROM vs_group s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        course_dept,
        student_id,
        student_cohort,
        COUNT(*) AS cnt,
        COUNT(DISTINCT student_cohort) AS distinct_student_cohorts,
        COUNT(DISTINCT term) AS distinct_terms,
        ARRAY_SORT(COLLECT_SET(course_level)) AS course_level_list,
        SUM(CASE WHEN student_cohort IN ('C2022', 'C2023') THEN 1 ELSE 0 END) AS n_student_cohort_in_c2022_c202
    FROM with_parent
    GROUP BY course_dept, student_id, student_cohort
),
joined_student AS (
    -- Enrich with dimension data
    SELECT
        s.course_dept,
        s.student_id,
        s.student_cohort,
        s.cnt,
        s.distinct_student_cohorts,
        s.distinct_terms,
        s.course_level_list,
        st.is_international AS student_is_international,
        st.cohort AS student_cohort_2,
        st.major AS student_major
    FROM rollup_base s
    JOIN students st ON st.id = s.student_id
    WHERE st.major = 'Math'
)
-- Subtotals with GROUPING SETS
SELECT
    student_id,
    distinct_terms,
    student_is_international,
    ARRAY_SORT(COLLECT_SET(student_cohort)) AS student_cohort_list
FROM joined_student
GROUP BY GROUPING SETS ((student_id, distinct_terms, student_is_international), (student_id), ())
ORDER BY student_id NULLS LAST, distinct_terms NULLS LAST, student_is_international NULLS LAST
