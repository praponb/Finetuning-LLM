WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        CASE WHEN grade_status IN ('WITHDRAWN', 'FAIL') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE term IN ('2024T1', '2025T1', '2024T2')
),
student_id_stats AS (
    -- Avg score per student_id
    SELECT student_id, AVG(score) AS avg_score
    FROM cleaned_enrollments
    GROUP BY student_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM cleaned_enrollments s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score > g.avg_score
),
summary AS (
    -- Summarise per group
    SELECT
        grade_status_group,
        student_id,
        term,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        COUNT(DISTINCT grade_status) AS distinct_grade_status,
        ROUND(SUM(score) / NULLIF(SUM(score_minus_avg), 0), 3) AS score_to_score_minus_avg_ratio
    FROM above_average
    WHERE score_minus_avg < 10.0
    GROUP BY 1, 2, 3
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM summary s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
        )
      AND s.distinct_grade_status < 8
)
SELECT *
FROM (
    SELECT student_id, distinct_grade_status, grade_status_group, n_rows
    FROM filtered
) src
PIVOT (
    MAX(n_rows) AS max_v, SUM(n_rows) AS sum_v
    FOR grade_status_group IN ('group_a', 'group_b', 'other')
)
