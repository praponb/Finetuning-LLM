WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 44.4) AS score_distance,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE student_id IS NOT NULL
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM base
    WHERE score_distance > (SELECT MIN(score_distance) FROM base)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_distance,
        grade_status_ref,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY grade_status, student_id)) AS distinct_course_id_in_grade_status_student_id,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY student_id ORDER BY score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER (PARTITION BY grade_status, student_id), 0), 4) AS score_distance_share_of_grade_status_student_id
    FROM qualified
),
joined_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.term,
        s.score,
        s.score_distance,
        s.grade_status_ref,
        s.distinct_course_id_in_grade_status_student_id,
        s.last_enrolled_at,
        s.score_distance_share_of_grade_status_student_id,
        st.cohort AS student_cohort
    FROM windowed s
    JOIN students st ON st.id = s.student_id
    WHERE st.cohort = 'C2024'
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        DATE_TRUNC('QUARTER', last_enrolled_at) AS last_enrolled_at_quarter,
        CONCAT_WS('|', grade_status_ref, student_cohort) AS grade_status_ref_student_cohort_label
    FROM joined_student
),
stats AS (
    -- Summarise per group
    SELECT
        distinct_course_id_in_grade_status_student_id,
        student_id,
        MAX(score_distance) AS max_score_distance,
        COUNT(*) FILTER (WHERE student_cohort = 'C2022') AS n_student_cohort_c2022,
        SUM(distinct_course_id_in_grade_status_student_id) AS total_distinct_course_id_in_grade_status_student
    FROM derived
    GROUP BY distinct_course_id_in_grade_status_student_id, student_id
    HAVING AVG(score_distance_share_of_grade_status_student_id) > 0.2
)
SELECT
    distinct_course_id_in_grade_status_student_id,
    n_student_cohort_c2022,
    total_distinct_course_id_in_grade_status_student,
    CASE WHEN n_student_cohort_c2022 >= 6 THEN 'A' WHEN n_student_cohort_c2022 >= 4 THEN 'B' ELSE 'C' END AS n_student_cohort_c2022_tier
FROM stats
ORDER BY n_student_cohort_c2022_tier DESC
