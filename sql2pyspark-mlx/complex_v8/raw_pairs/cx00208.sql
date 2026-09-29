WITH scoped AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CASE WHEN dept IN ('CS', 'Bio') THEN 'group_a' WHEN dept = 'Art' THEN 'group_b' ELSE 'other' END AS dept_group,
        CONCAT(course_code, '#', CAST(credits AS STRING)) AS course_code_ref,
        LOWER(TRIM(level)) AS level_clean
    FROM courses
    WHERE UPPER(TRIM(course_code)) LIKE 'C%'
),
unique_rows AS (
    -- Keep the latest row per dept_group, dept
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        dept_group,
        course_code_ref,
        level_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY dept_group, dept ORDER BY credits DESC, id DESC) AS rn
        FROM scoped
    ) ranked
    WHERE rn = 1
),
deduped AS (
    -- Keep the earliest row per dept_group, dept
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        dept_group,
        course_code_ref,
        level_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY dept_group, dept ORDER BY credits, id) AS rn
        FROM unique_rows
    ) ranked
    WHERE rn = 1
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.dept,
        s.credits,
        s.dept_group,
        s.course_code_ref,
        s.level_clean,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at
    FROM deduped s
    LEFT JOIN enrollments e ON e.course_id = s.id
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.dept_group,
        s.course_code_ref,
        s.level_clean,
        s.enrollment_id,
        s.enrollment_student_id,
        s.enrollment_grade_status,
        s.enrollment_enrolled_at,
        e.id AS enrollment_id_2,
        e.student_id AS enrollment_student_id_2,
        e.score AS enrollment_score_2,
        e.enrolled_at AS enrollment_enrolled_at_2
    FROM with_children s
    JOIN enrollments e ON e.course_id = s.id AND e.enrolled_at < TIMESTAMP '2025-02-24 09:00:00'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(enrollment_enrolled_at, 3) OVER (PARTITION BY enrollment_id_2 ORDER BY enrollment_enrolled_at DESC, id, enrollment_id, enrollment_id_2) AS prev3_enrollment_enrolled_at,
        ROW_NUMBER() OVER (ORDER BY enrollment_score_2 NULLS LAST, id, enrollment_id, enrollment_id_2) AS enrollment_score_2_seq,
        ROUND(enrollment_score_2 / NULLIF(SUM(enrollment_score_2) OVER (PARTITION BY enrollment_grade_status), 0), 4) AS enrollment_score_2_share_of_enrollment_grade_sta
    FROM detail_rows
)
SELECT
    enrollment_id,
    enrollment_id_2,
    enrollment_student_id_2,
    GROUPING_ID() AS grouping_level,
    SUM(enrollment_score_2_seq) AS total_enrollment_score_2_seq,
    ROUND(STDDEV(enrollment_score_2_share_of_enrollment_grade_sta), 2) AS stddev_enrollment_score_2_share_of_enrollment_gr,
    COUNT(*) FILTER (WHERE enrollment_grade_status = 'WITHDRAWN') AS n_enrollment_grade_status_with
FROM sequenced
GROUP BY CUBE(enrollment_id, enrollment_id_2, enrollment_student_id_2)
