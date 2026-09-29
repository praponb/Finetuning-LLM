WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2025T1', '2025T2') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group,
        CASE WHEN score >= 57.6 THEN 'HIGH' WHEN score >= 11.3 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE course_id <= 5
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(score) OVER (PARTITION BY grade_status) AS grade_status_max_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status), 0), 4) AS score_share_of_grade_status
    FROM prepared
),
course_id_stats AS (
    -- Median grade_status_max_score per course_id
    SELECT course_id, PERCENTILE_APPROX(grade_status_max_score, 0.5) AS median_grade_status_max_score
    FROM windowed
    GROUP BY course_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_grade_status_max_score,
        ROUND(s.grade_status_max_score - g.median_grade_status_max_score, 2) AS grade_status_max_score_minus_median
    FROM windowed s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.grade_status_max_score >= g.median_grade_status_max_score
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM above_average
    WHERE score_share_of_grade_status > (SELECT AVG(score_share_of_grade_status) FROM above_average)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_group,
        score_tier,
        grade_status_max_score,
        score_share_of_grade_status,
        median_grade_status_max_score,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month
    FROM screened
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        score,
        term_group,
        score_tier,
        GREATEST(grade_status_max_score, score) AS greatest_grade_status_max_score_score
    FROM tagged
    WHERE median_grade_status_max_score < 800.0
),
screened_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE score > (SELECT AVG(score) FROM labeled)
)
SELECT id, metric, amount
FROM (
    SELECT id, greatest_grade_status_max_score_score, score
    FROM screened_2
) src
UNPIVOT (
    amount FOR metric IN (greatest_grade_status_max_score_score, score)
) unp
ORDER BY id, metric
