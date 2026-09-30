WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 38.1) AS score_distance,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label
    FROM enrollments
    WHERE course_id <= 7
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        QUARTER(enrolled_at) AS enrolled_at_quarter,
        ROUND(score_distance * 0.25, 2) AS score_distance_adj,
        ROUND(score_distance * 0.9, 2) AS score_distance_adj_2
    FROM cleaned_enrollments
    WHERE student_id IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY grade_status ORDER BY enrolled_at_quarter DESC, id) AS enrolled_at_quarter_decile,
        LAST_VALUE(score_distance_adj_2) OVER (PARTITION BY course_id, grade_status ORDER BY score_distance_adj_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_distance_adj_2,
        MAX(score_distance_adj_2) OVER (ORDER BY enrolled_at_quarter, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_distance_adj_2_max_2_prior
    FROM annotated
),
stats AS (
    -- Group-level totals
    SELECT
        term,
        MIN(score_distance) AS min_score_distance,
        MAX(last_score_distance_adj_2) AS max_last_score_distance_adj_2
    FROM sequenced
    WHERE enrolled_at_quarter BETWEEN 2 AND 3
    GROUP BY term
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        term,
        max_last_score_distance_adj_2,
        ROUND(AVG(min_score_distance) OVER (PARTITION BY term ORDER BY min_score_distance, term ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS min_score_distance_avg_6_prior,
        LAG(min_score_distance) OVER (PARTITION BY term ORDER BY min_score_distance, term) AS prev_min_score_distance,
        ROUND(AVG(min_score_distance) OVER (PARTITION BY term), 2) AS term_avg_min_score_distance
    FROM stats
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        UPPER(TRIM(term)) AS term_clean
    FROM windowed
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(prev_min_score_distance - 40.0) AS prev_min_score_distance_distance
    FROM enriched
    WHERE term_slug IS NOT NULL
)
-- Final projection
SELECT
    term,
    min_score_distance_avg_6_prior,
    prev_min_score_distance,
    term_avg_min_score_distance,
    term_slug,
    prev_min_score_distance_distance,
    LOWER(TRIM(term)) AS term_clean_2
FROM enriched_2
WHERE term_slug IS NOT NULL
ORDER BY term_slug
