WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE term = '2024T2'
      AND grade_status IN ('INCOMPLETE', 'FAIL')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        term,
        enrolled_at,
        term_slug,
        ROUND(enrolled_at_quarter / NULLIF(SUM(enrolled_at_quarter) OVER (PARTITION BY enrolled_at_quarter), 0), 4) AS enrolled_at_quarter_share_of_enrolled_at_quarter,
        COUNT(*) OVER w_enrolled_at_quarter AS rows_in_enrolled_at_quarter,
        LEAD(enrolled_at_quarter) OVER (PARTITION BY term ORDER BY enrolled_at DESC, id) AS next_enrolled_at_quarter
    FROM prepared_enrollments
    WINDOW w_enrolled_at_quarter AS (PARTITION BY enrolled_at_quarter)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(rows_in_enrolled_at_quarter) OVER (PARTITION BY TO_DATE(enrolled_at)) AS enrolled_at_to_date_max_rows_in_enrolled_at_quar,
        next_enrolled_at_quarter - LAG(next_enrolled_at_quarter) OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY enrolled_at_quarter_share_of_enrolled_at_quarter DESC, id) AS next_enrolled_at_quarter_change,
        FIRST_VALUE(next_enrolled_at_quarter) OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY enrolled_at_quarter_share_of_enrolled_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next_enrolled_at_quarter,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY enrolled_at_quarter_share_of_enrolled_at_quarter DESC, id), 3) AS enrolled_at_quarter_share_of_enrolled_at_quarter_2,
        MIN(next_enrolled_at_quarter) OVER (PARTITION BY TO_DATE(enrolled_at)) AS enrolled_at_to_date_min_next_enrolled_at_quarter,
        NTH_VALUE(rows_in_enrolled_at_quarter, 2) OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY enrolled_at_quarter_share_of_enrolled_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_enrolled_at_quarter
    FROM metrics
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(next_enrolled_at_quarter, 0) AS next_enrolled_at_quarter_filled
    FROM ranked
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    enrolled_at,
    term_slug,
    rows_in_enrolled_at_quarter,
    next_enrolled_at_quarter,
    enrolled_at_to_date_max_rows_in_enrolled_at_quar,
    first_next_enrolled_at_quarter,
    enrolled_at_quarter_share_of_enrolled_at_quarter_2,
    enrolled_at_to_date_min_next_enrolled_at_quarter,
    COALESCE(term, 'UNKNOWN') AS term_or_unknown,
    LOWER(TRIM(term_slug)) AS term_slug_clean
FROM derived
ORDER BY term, id
LIMIT 10
