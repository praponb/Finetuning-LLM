WITH cleaned AS (
    -- Filter students
    SELECT
        id,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE is_international = FALSE
      AND UPPER(TRIM(student_name)) LIKE 'B%'
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.course_id IS NOT NULL
              AND HOUR(e.enrolled_at) BETWEEN 7 AND 11
        )
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(gpa) OVER w_is_international AS is_international_min_gpa,
        COUNT(*) OVER w_is_international AS rows_in_is_international,
        RANK() OVER (PARTITION BY is_international, major ORDER BY gpa DESC, id) AS gpa_rank
    FROM screened
    WINDOW w_is_international AS (PARTITION BY is_international)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE gpa_rank <= 5
      AND is_international_min_gpa < 22.7
),
totals AS (
    -- Summarise per group
    SELECT
        is_international,
        ROUND(AVG(gpa_rank), 2) AS avg_gpa_rank,
        ROUND(SUM(gpa) / NULLIF(SUM(is_international_min_gpa), 0), 3) AS gpa_to_is_international_min_gpa_ratio
    FROM top_rows
    GROUP BY is_international
),
annotated AS (
    -- Label and score rows
    SELECT
        is_international,
        avg_gpa_rank,
        gpa_to_is_international_min_gpa_ratio,
        COALESCE(avg_gpa_rank, 0.0) AS avg_gpa_rank_filled,
        CASE WHEN avg_gpa_rank >= 8.2 THEN 'HIGH' WHEN avg_gpa_rank >= 3.2 THEN 'MEDIUM' ELSE 'LOW' END AS avg_gpa_rank_tier,
        ABS(avg_gpa_rank - 4.2) AS avg_gpa_rank_distance
    FROM totals
)
-- Final projection
SELECT
    is_international,
    avg_gpa_rank,
    gpa_to_is_international_min_gpa_ratio,
    avg_gpa_rank_filled,
    avg_gpa_rank_tier,
    avg_gpa_rank_distance,
    FLOOR(avg_gpa_rank_distance / 2) * 2 AS avg_gpa_rank_distance_bucket
FROM annotated
ORDER BY avg_gpa_rank_filled DESC, is_international
LIMIT 5
