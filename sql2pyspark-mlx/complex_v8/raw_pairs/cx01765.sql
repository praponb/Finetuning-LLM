WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE grade_status <> 'WITHDRAWN'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY course_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        RANK() OVER (PARTITION BY term ORDER BY enrolled_at DESC) AS enrolled_at_rank,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, DATE_TRUNC('MONTH', enrolled_at) ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        ROUND(score - AVG(score) OVER w_course_id, 2) AS score_vs_course_id_avg,
        LAG(enrolled_at, 2) OVER (PARTITION BY YEAR(enrolled_at), term ORDER BY score, id) AS prev2_enrolled_at,
        MAX(score) OVER (PARTITION BY YEAR(enrolled_at)) AS enrolled_at_year_max_score,
        RANK() OVER w_course_id_enrolled_at_month_2 AS score_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY YEAR(enrolled_at)), 0), 4) AS score_share_of_enrolled_at_year,
        COUNT(*) OVER (PARTITION BY YEAR(enrolled_at)) AS rows_in_enrolled_at_year
    FROM cleaned_enrollments
    WINDOW w_course_id AS (PARTITION BY course_id), w_course_id_enrolled_at_month_2 AS (PARTITION BY course_id, DATE_TRUNC('MONTH', enrolled_at) ORDER BY score DESC)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        score_vs_course_id_avg - last_known_score AS score_vs_course_id_avg_minus_last_known_score,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label_2
    FROM ranked
    WHERE term IN ('2024T2', '2025T1')
)
-- Final projection
SELECT
    id,
    course_id,
    score,
    last_known_score,
    score_share_of_course_id,
    enrolled_at_rank,
    last_known_enrolled_at,
    prev2_enrolled_at,
    enrolled_at_year_max_score,
    score_rank,
    score_share_of_enrolled_at_year,
    rows_in_enrolled_at_year,
    score_vs_course_id_avg_minus_last_known_score,
    enrolled_at_label,
    enrolled_at_label_2,
    LOWER(REGEXP_REPLACE(TRIM(enrolled_at_label_2), ' ', '_')) AS enrolled_at_label_2_slug
FROM scored
WHERE rows_in_enrolled_at_year <= 12
ORDER BY enrolled_at_label_2 DESC, id NULLS LAST
LIMIT 10
