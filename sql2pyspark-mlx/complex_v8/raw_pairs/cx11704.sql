WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix,
        ABS(score - 34.2) AS score_distance,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE student_id >= 14
      AND term = '2025T2'
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM recent
    WHERE score_distance > (SELECT MIN(score_distance) FROM recent)
),
totals AS (
    -- Group-level totals
    SELECT
        student_id,
        TO_DATE(enrolled_at) AS enrolled_at_day,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN term <> '2025T2' THEN 1 ELSE 0 END) AS n_term_2025t2,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        COUNT(*) AS cnt
    FROM qualified
    GROUP BY student_id, TO_DATE(enrolled_at)
    HAVING SUM(score) > 34.2
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(cnt, 1, 0) OVER w_student_id_by_n_term_2025t2 AS next_cnt,
        SUM(distinct_ids) OVER (PARTITION BY distinct_ids, student_id) AS distinct_ids_student_id_sum_distinct_ids
    FROM totals
    WINDOW w_student_id_by_n_term_2025t2 AS (PARTITION BY student_id ORDER BY n_term_2025t2 DESC, student_id, enrolled_at_day)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, COUNT(*) AS enrollment_count, COUNT(*) AS enrollment_count_2
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2
    FROM metrics s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(enrollment_count) OVER (ORDER BY cnt, student_id, enrolled_at_day ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS enrollment_count_sum_1_prior,
        MIN(n_term_2025t2) OVER (PARTITION BY student_id, distinct_course_ids) AS student_id_distinct_course_ids_min_n_term_2025t2
    FROM with_counts
),
with_parent AS (
    -- Join students
    SELECT
        s.student_id,
        s.enrolled_at_day,
        s.n_term_2025t2,
        s.term_list,
        s.cnt,
        s.next_cnt,
        s.distinct_ids_student_id_sum_distinct_ids,
        s.enrollment_count,
        s.enrollment_count_2,
        s.enrollment_count_sum_1_prior,
        s.student_id_distinct_course_ids_min_n_term_2025t2,
        st.cohort AS student_cohort
    FROM sequenced s
    JOIN students st ON st.id = s.student_id AND st.major = 'Biz'
)
-- Final projection
SELECT
    student_id,
    enrolled_at_day,
    n_term_2025t2,
    next_cnt,
    distinct_ids_student_id_sum_distinct_ids,
    enrollment_count,
    enrollment_count_sum_1_prior,
    student_id_distinct_course_ids_min_n_term_2025t2,
    YEAR(enrolled_at_day) AS enrolled_at_day_year,
    LOWER(TRIM(student_cohort)) AS student_cohort_clean
FROM with_parent
ORDER BY student_id
