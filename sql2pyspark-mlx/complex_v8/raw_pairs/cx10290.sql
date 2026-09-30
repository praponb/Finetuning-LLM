WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 63.9) AS score_distance,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE (student_id <= 2 OR score BETWEEN 17.4 AND 92.4)
),
annotated AS (
    -- Label and score rows
    SELECT *, DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM scoped
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(score, 1, 0) OVER w_term_by_score_distance AS next_score,
        LEAD(score_distance, 2) OVER (PARTITION BY term ORDER BY score_distance ASC, id) AS next2_score_distance,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY term)) AS distinct_grade_status_in_term
    FROM annotated
    WINDOW w_term_by_score_distance AS (PARTITION BY term ORDER BY score_distance ASC, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(days_since_enrolled_at) OVER (PARTITION BY grade_status, YEAR(enrolled_at_week)) AS grade_status_enrolled_at_week_year_min_days_sinc,
        ROUND(next2_score_distance / NULLIF(SUM(next2_score_distance) OVER (PARTITION BY grade_status, YEAR(enrolled_at_week)), 0), 4) AS next2_score_distance_share_of_grade_status_enrol,
        MAX(days_since_enrolled_at) OVER (PARTITION BY grade_status, YEAR(enrolled_at_week)) AS grade_status_enrolled_at_week_year_max_days_sinc,
        ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY enrolled_at_week, id) AS enrolled_at_week_seq,
        NTILE(4) OVER (PARTITION BY grade_status ORDER BY enrolled_at_week, id) AS enrolled_at_week_quartile
    FROM with_trends
),
annotated_2 AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(next_score - 55.6) AS next_score_distance,
        LEAST(score, days_since_enrolled_at) AS least_score_days_since_enrolled_at
    FROM metrics
    WHERE course_id <= 8
),
totals AS (
    -- Summarise per group
    SELECT
        student_id,
        term,
        TO_DATE(enrolled_at) AS enrolled_at_day,
        PERCENTILE_APPROX(grade_status_enrolled_at_week_year_max_days_sinc, 0.5) AS median_grade_status_enrolled_at_week_year_max_da,
        MAX(next_score_distance) AS max_next_score_distance,
        MAX(least_score_days_since_enrolled_at) AS max_least_score_days_since_enrolled_at
    FROM annotated_2
    GROUP BY student_id, term, TO_DATE(enrolled_at)
),
screened AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM totals s
    WHERE EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.enrolled_on BETWEEN DATE '2024-04-01' AND DATE '2025-06-27'
              AND st.major IN ('CS', 'Art', 'Math')
        )
)
-- Final projection
SELECT
    student_id,
    enrolled_at_day,
    median_grade_status_enrolled_at_week_year_max_da,
    max_next_score_distance,
    max_least_score_days_since_enrolled_at
FROM screened
ORDER BY max_next_score_distance NULLS LAST
