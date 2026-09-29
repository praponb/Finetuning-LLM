WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE (student_id <= 13 OR enrolled_at < TIMESTAMP '2025-02-24 13:00:00')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        course_id,
        grade_status,
        enrolled_at,
        NTILE(4) OVER (ORDER BY score, id) AS score_quartile,
        NTILE(3) OVER (ORDER BY score, id) AS score_tercile,
        MAX(score) OVER w_student_id_course_id AS student_id_course_id_max_score,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY student_id, course_id)) AS distinct_grade_status_in_student_id_course_id,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_all_by_score)) / 60, 1) AS minutes_since_prev_enrolled_at,
        SUM(score) OVER (ORDER BY score, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_sum_7
    FROM cleaned
    WINDOW w_student_id_course_id AS (PARTITION BY student_id, course_id), w_all_by_score AS (ORDER BY score, id)
),
enriched AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.score_tercile,
        s.student_id_course_id_max_score,
        s.distinct_grade_status_in_student_id_course_id,
        s.minutes_since_prev_enrolled_at,
        c.level AS course_level,
        c.course_code
    FROM with_windows s
    JOIN courses c ON c.id = s.course_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT score_tercile, course_level, grade_status, distinct_grade_status_in_student_id_course_id
    FROM enriched
) src
PIVOT (
    SUM(distinct_grade_status_in_student_id_course_id) AS total, COUNT(distinct_grade_status_in_student_id_course_id) AS n
    FOR grade_status IN ('PASS', 'INCOMPLETE')
)
