WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m
    FROM enrollments
    WHERE score > 26.2
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        term_grade_status_label,
        enrolled_at_plus_30m,
        COALESCE(score, 0.0) AS score_filled,
        COALESCE(score, 0.0) AS score_filled_2,
        COALESCE(score, 0.0) AS score_filled_3
    FROM scoped
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        CASE WHEN score_filled_2 >= 55.0 THEN 'HIGH' WHEN score_filled_2 >= 30.0 THEN 'MEDIUM' ELSE 'LOW' END AS score_filled_2_tier
    FROM derived
),
course_id_stats AS (
    -- Avg grade_status_len per course_id
    SELECT course_id, AVG(grade_status_len) AS avg_grade_status_len
    FROM annotated
    GROUP BY course_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_grade_status_len,
        ROUND(s.grade_status_len - g.avg_grade_status_len, 2) AS grade_status_len_minus_avg
    FROM annotated s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.grade_status_len > g.avg_grade_status_len
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM compared s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term = '2025T2'
              AND e.grade_status <> 'PASS'
        )
),
annotated_2 AS (
    -- Label and score rows
    SELECT
        id,
        grade_status,
        term_grade_status_label,
        enrolled_at_plus_30m,
        score_filled,
        score_filled_2,
        score_filled_3,
        grade_status_len,
        score_filled_2_tier,
        grade_status_len_minus_avg,
        CONCAT(score_filled_2_tier, '#', CAST(student_id AS STRING)) AS score_filled_2_tier_ref,
        CASE WHEN score_filled_2_tier IN ('MEDIUM', 'HIGH') THEN 'group_a' WHEN score_filled_2_tier = 'LOW' THEN 'group_b' ELSE 'other' END AS score_filled_2_tier_group
    FROM qualified
),
score_filled_2_tier_group_stats AS (
    -- Avg score_filled_2 per score_filled_2_tier_group
    SELECT score_filled_2_tier_group, AVG(score_filled_2) AS avg_score_filled_2
    FROM annotated_2
    GROUP BY score_filled_2_tier_group
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_score_filled_2,
        ROUND(s.score_filled_2 - g.avg_score_filled_2, 2) AS score_filled_2_minus_avg
    FROM annotated_2 s
    JOIN score_filled_2_tier_group_stats g ON g.score_filled_2_tier_group = s.score_filled_2_tier_group
    WHERE s.score_filled_2 > g.avg_score_filled_2
)
SELECT
    id,
    grade_status,
    term_grade_status_label,
    enrolled_at_plus_30m,
    score_filled,
    score_filled_3,
    score_filled_2_tier,
    score_filled_2_tier_ref,
    score_filled_2_tier_group,
    score_filled_2_minus_avg,
    GREATEST(score_filled_2, score_filled_2_minus_avg) AS greatest_score_filled_2_score_filled_2_minus_avg,
    DATE_FORMAT(enrolled_at_plus_30m, 'yyyy-MM-dd') AS enrolled_at_plus_30m_label
FROM outliers
ORDER BY score_filled_3 NULLS LAST, grade_status DESC NULLS FIRST
