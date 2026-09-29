WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label,
        CASE WHEN score >= 62.5 THEN 'large' WHEN score >= 38.3 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE (student_id >= 9 OR term = '2025T1')
),
score_tier_stats AS (
    -- Avg score per score_tier
    SELECT score_tier, AVG(score) AS avg_score
    FROM filtered_enrollments
    GROUP BY score_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM filtered_enrollments s
    JOIN score_tier_stats g ON g.score_tier = s.score_tier
    WHERE s.score > g.avg_score
),
latest AS (
    -- Keep the latest row per score_tier
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        grade_status_term_label,
        term_grade_status_label,
        score_tier,
        avg_score,
        score_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY score_tier ORDER BY avg_score DESC, id DESC) AS row_num
        FROM compared
    ) ranked
    WHERE row_num = 1
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_term_label,
        s.avg_score,
        s.score_minus_avg,
        st.student_name,
        c.credits AS course_credits
    FROM latest s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
    WHERE LENGTH(TRIM(st.student_name)) >= 4
)
SELECT id, metric_name, reading
FROM (
    SELECT id, score, score_minus_avg, avg_score
    FROM joined
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (score, score_minus_avg, avg_score)
) unp
ORDER BY id, metric_name
