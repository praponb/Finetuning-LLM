WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        CASE WHEN term IN ('2024T1', '2024T2') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE (course_id >= 9 OR enrolled_at < TIMESTAMP '2025-02-18 09:00:00')
),
term_group_stats AS (
    -- Median score per term_group
    SELECT term_group, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM recent_enrollments
    GROUP BY term_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM recent_enrollments s
    JOIN term_group_stats g ON g.term_group = s.term_group
    WHERE s.score >= g.median_score
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM compared s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
            WHERE e.term IN ('2025T1', '2024T2')
              AND e.student_id IS NOT NULL
        )
      AND s.term_group = 'group_a'
),
latest AS (
    -- Keep the latest row per course_id, student_id
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        enrolled_at_year,
        enrolled_at_label,
        term_group,
        median_score,
        score_minus_median
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id, student_id ORDER BY score DESC, id DESC) AS row_num
        FROM with_activity
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    enrolled_at,
    enrolled_at_label,
    term_group,
    score_minus_median,
    LOWER(REGEXP_REPLACE(TRIM(enrolled_at_label), ' ', '_')) AS enrolled_at_label_slug,
    DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label_2
FROM latest
ORDER BY enrolled_at_label DESC NULLS FIRST, course_id
