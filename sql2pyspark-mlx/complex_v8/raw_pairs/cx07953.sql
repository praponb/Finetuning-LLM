WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CASE WHEN score >= 62.5 THEN 'A' WHEN score >= 11.3 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE student_id <= 14
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY term ORDER BY score ASC), 3) AS score_cume_dist,
        ROUND(AVG(score) OVER (PARTITION BY term ORDER BY score ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY course_id, student_id)) AS distinct_term_in_course_id_student_id,
        ROUND(score / NULLIF(SUM(score) OVER w_course_id_student_id, 0), 4) AS score_share_of_course_id_student_id
    FROM filtered_enrollments
    WINDOW w_course_id_student_id AS (PARTITION BY course_id, student_id)
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id IS NOT NULL
        )
)
SELECT id, field, reading
FROM (
    SELECT id, score_share_of_course_id_student_id, score_cume_dist
    FROM without_matches
) src
UNPIVOT (
    reading FOR field IN (score_share_of_course_id_student_id, score_cume_dist)
) unp
