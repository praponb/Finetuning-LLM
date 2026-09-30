WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score >= 6.2
      AND grade_status <> 'PASS'
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM scoped_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(score) OVER (PARTITION BY course_id) AS course_id_sum_score,
        MAX(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_max_2_prior,
        NTH_VALUE(score, 2) OVER (ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        SUM(score) OVER (ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        SUM(score) OVER (ORDER BY score ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_sum_3
    FROM without_matches
)
SELECT
    COALESCE(term, 'ALL') AS term,
    course_id,
    GROUPING_ID() AS gid,
    PERCENTILE_APPROX(course_id_sum_score, 0.5) AS median_course_id_sum_score,
    MAX(course_id_sum_score) AS max_course_id_sum_score,
    SUM(CASE WHEN term = '2025T1' THEN score_max_2_prior ELSE 0 END) AS score_max_2_prior_term_2025t1
FROM windowed
GROUP BY ROLLUP(term, course_id)
ORDER BY term NULLS LAST, course_id NULLS LAST
