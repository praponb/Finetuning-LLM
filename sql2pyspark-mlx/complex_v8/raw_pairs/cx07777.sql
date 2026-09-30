WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score < 78.8
      AND grade_status IN ('INCOMPLETE', 'PASS')
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM recent_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
      AND s.term <> '2025T1'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status ORDER BY score DESC, id), 3) AS score_cume_dist,
        ROUND(AVG(score) OVER (PARTITION BY course_id), 2) AS course_id_avg_score,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY course_id)) AS distinct_term_in_course_id,
        ROUND(CUME_DIST() OVER w_grade_status_by_score_2, 3) AS score_cume_dist_2
    FROM filtered
    WINDOW w_grade_status_by_score_2 AS (PARTITION BY grade_status ORDER BY score ASC, id)
),
filtered_2 AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND UPPER(TRIM(c.course_code)) LIKE 'C%'
              AND c.level IN ('intro', 'core')
        )
)
-- Subtotals with GROUPING SETS
SELECT
    grade_status,
    distinct_term_in_course_id,
    course_id,
    GROUPING_ID() AS gid,
    ROUND(AVG(course_id_avg_score), 2) AS avg_course_id_avg_score,
    COUNT(DISTINCT term) AS distinct_terms,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
FROM filtered_2
GROUP BY GROUPING SETS ((grade_status, distinct_term_in_course_id, course_id), (grade_status), (distinct_term_in_course_id), ())
ORDER BY grade_status NULLS LAST, distinct_term_in_course_id NULLS LAST, course_id NULLS LAST
