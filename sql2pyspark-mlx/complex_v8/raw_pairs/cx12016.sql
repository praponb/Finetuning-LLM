WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label,
        enrolled_at + INTERVAL 7 DAYS AS enrolled_at_plus_7d
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 2)
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at_label,
        s.enrolled_at_plus_7d,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score
    FROM scoped_enrollments s
    JOIN enrollments e ON e.student_id = s.student_id
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.total_enrollment_score
    FROM exploded_detail s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at_label,
        s.enrolled_at_plus_7d,
        s.enrollment_id,
        s.enrollment_score,
        s.total_enrollment_score,
        e.id AS enrollment_id_2,
        e.score AS enrollment_score_2,
        e.enrolled_at AS enrollment_enrolled_at,
        e.course_id AS enrollment_course_id
    FROM summarized_rows s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(total_enrollment_score) OVER w_course_id_enrollment_course AS course_id_enrollment_course_id_sum_total_enrollm,
        LAST_VALUE(enrollment_score) IGNORE NULLS OVER (PARTITION BY course_id ORDER BY enrollment_score_2 ASC, id, enrollment_id, enrollment_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrollment_score,
        COUNT(*) OVER (PARTITION BY course_id, enrollment_course_id) AS rows_in_course_id_enrollment_course_id,
        NTH_VALUE(enrollment_score_2, 2) OVER (PARTITION BY course_id ORDER BY enrollment_score_2 ASC, id, enrollment_id, enrollment_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrollment_score_2
    FROM line_level
    WINDOW w_course_id_enrollment_course AS (PARTITION BY course_id, enrollment_course_id)
),
enrollment_course_id_stats AS (
    -- Max enrollment_score_2 per enrollment_course_id
    SELECT enrollment_course_id, MAX(enrollment_score_2) AS max_enrollment_score_2
    FROM sequenced
    GROUP BY enrollment_course_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_enrollment_score_2,
        ROUND(s.enrollment_score_2 - g.max_enrollment_score_2, 2) AS enrollment_score_2_minus_max
    FROM sequenced s
    JOIN enrollment_course_id_stats g ON g.enrollment_course_id = s.enrollment_course_id
    WHERE s.enrollment_score_2 = g.max_enrollment_score_2
),
with_details AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrollment_id,
        s.enrollment_score,
        s.enrollment_id_2,
        s.enrollment_score_2,
        s.enrollment_enrolled_at,
        s.enrollment_course_id,
        s.last_known_enrollment_score,
        s.rows_in_course_id_enrollment_course_id,
        s.second_enrollment_score_2,
        s.max_enrollment_score_2,
        s.enrollment_score_2_minus_max,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international,
        st.cohort AS student_cohort,
        c.level AS course_level,
        c.credits AS course_credits
    FROM vs_group s
    JOIN students st ON st.id = s.student_id AND st.major IS NOT NULL
    JOIN courses c ON c.id = s.enrollment_course_id
    WHERE st.cohort NOT IN ('C2024', 'C2021')
)
SELECT id, enrollment_id, measure, metric_value
FROM (
    SELECT id, enrollment_id, enrollment_score_2_minus_max, max_enrollment_score_2, score
    FROM with_details
) src
UNPIVOT (
    metric_value FOR measure IN (enrollment_score_2_minus_max AS m_enrollment_score_2_minus_max, max_enrollment_score_2 AS m_max_enrollment_score_2, score AS m_score)
) unp
