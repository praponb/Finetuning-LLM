WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 69.6 THEN 'large' WHEN score >= 43.5 THEN 'medium' ELSE 'small' END AS score_tier,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 10 AND 15
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY term, grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        FIRST_VALUE(enrolled_at_dayofweek) IGNORE NULLS OVER (PARTITION BY term, grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at_dayofweek,
        ROUND(AVG(enrolled_at_dayofweek) OVER (PARTITION BY course_id, score_tier), 2) AS course_id_score_tier_avg_enrolled_at_dayofweek,
        MAX(score) OVER (PARTITION BY course_id, score_tier) AS course_id_score_tier_max_score,
        ROUND(CUME_DIST() OVER w_student_id_by_score, 3) AS score_cume_dist,
        ROUND(enrolled_at_dayofweek / NULLIF(SUM(enrolled_at_dayofweek) OVER (PARTITION BY term), 0), 4) AS enrolled_at_dayofweek_share_of_term,
        MAX(enrolled_at_dayofweek) OVER (PARTITION BY student_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_enrolled_at_dayofweek_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status, enrolled_at_dayofweek ORDER BY score DESC NULLS FIRST, id), 3) AS score_cume_dist_2,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term
    FROM recent
    WINDOW w_student_id_by_score AS (PARTITION BY student_id ORDER BY score, id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    score_tier,
    enrolled_at_dayofweek,
    first_enrolled_at,
    course_id_score_tier_max_score,
    score_cume_dist,
    enrolled_at_dayofweek_share_of_term,
    max_enrolled_at_dayofweek_so_far,
    score_cume_dist_2,
    rows_in_term,
    TO_DATE(enrolled_at) AS enrolled_at_date,
    LENGTH(TRIM(grade_status)) AS grade_status_len
FROM metrics
ORDER BY enrolled_at_date NULLS LAST
