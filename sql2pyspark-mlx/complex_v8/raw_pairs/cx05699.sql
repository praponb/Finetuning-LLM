WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CASE WHEN score >= 81.4 THEN 'HIGH' WHEN score >= 34.2 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE term <> '2025T2'
      AND grade_status IS NOT NULL
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score
    FROM cleaned_enrollments s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.enrollment_id,
        s.enrollment_enrolled_at,
        s.enrollment_score,
        e.id AS enrollment_id_2,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score_2,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id
    FROM detail_rows s
    JOIN enrollments e ON e.course_id = s.course_id AND e.term IS NOT NULL
),
student_id_stats AS (
    -- Avg score per student_id
    SELECT student_id, AVG(score) AS avg_score
    FROM line_level
    GROUP BY student_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM line_level s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score > g.avg_score
),
summary AS (
    -- Group-level totals
    SELECT
        student_id,
        YEAR(enrolled_at) AS enrolled_at_year,
        ROUND(STDDEV(score_minus_avg), 2) AS stddev_score_minus_avg,
        ROUND(AVG(avg_score), 2) AS avg_avg_score,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list
    FROM vs_group
    WHERE enrollment_grade_status = 'INCOMPLETE'
    GROUP BY 1, 2
    HAVING SUM(score) > 26.2
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM summary s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term IS NOT NULL
              AND e.enrolled_at < TIMESTAMP '2025-02-12 09:00:00'
        )
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, st.major AS student_major, st.student_name
    FROM qualified s
    JOIN students st ON st.id = s.student_id
)
SELECT *
FROM (
    SELECT enrolled_at_year, student_major, stddev_score_minus_avg
    FROM with_details
) src
PIVOT (
    SUM(stddev_score_minus_avg)
    FOR student_major IN ('Art', 'Biz')
)
