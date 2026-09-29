WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        ABS(score - 38.1) AS score_distance
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-02-06'
      AND course_id <= 11
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref,
        LOWER(TRIM(term)) AS term_clean,
        ABS(score - 44.4) AS score_distance_2
    FROM cleaned_enrollments
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.score_distance,
        s.term_ref,
        s.term_clean,
        s.score_distance_2,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id
    FROM tagged s
    JOIN enrollments e ON e.student_id = s.student_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(score_distance_2) OVER (PARTITION BY enrollment_grade_status) AS enrollment_grade_status_max_score_distance_2,
        DENSE_RANK() OVER (PARTITION BY enrollment_id, student_id ORDER BY enrolled_at ASC) AS enrolled_at_dense_rank,
        MAX(score_distance) OVER (PARTITION BY enrollment_grade_status) AS enrollment_grade_status_max_score_distance,
        ROUND(score_distance_2 - AVG(score_distance_2) OVER (PARTITION BY enrollment_grade_status), 2) AS score_distance_2_vs_enrollment_grade_status_avg,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY enrollment_id, student_id ORDER BY enrolled_at ASC, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at
    FROM detail_rows
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(score_distance) OVER w_student_id AS student_id_min_score_distance,
        LEAD(enrolled_at) OVER (PARTITION BY enrollment_course_id, DATE_TRUNC('MONTH', enrolled_at) ORDER BY score_distance DESC, id, enrollment_id) AS next_enrolled_at
    FROM windowed
    WINDOW w_student_id AS (PARTITION BY student_id)
)
SELECT
    course_id,
    enrollment_course_id,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(enrollment_term)) AS enrollment_term_list
FROM with_windows
GROUP BY GROUPING SETS ((course_id, enrollment_course_id), (course_id), (enrollment_course_id))
ORDER BY course_id NULLS LAST, enrollment_course_id NULLS LAST
