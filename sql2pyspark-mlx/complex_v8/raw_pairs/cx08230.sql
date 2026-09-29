WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        QUARTER(enrolled_at) AS enrolled_at_quarter,
        LENGTH(TRIM(term)) AS term_len,
        MONTH(enrolled_at) AS enrolled_at_month
    FROM enrollments
    WHERE grade_status IN ('WITHDRAWN', 'FAIL')
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(enrolled_at_month, 0) AS enrolled_at_month_filled,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        ABS(enrolled_at_quarter - 3) AS enrolled_at_quarter_distance
    FROM cleaned
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(3) OVER (ORDER BY enrolled_at_month_filled, id) AS enrolled_at_month_filled_tercile,
        LEAD(enrolled_at, 2) OVER (ORDER BY term_len DESC NULLS FIRST, id) AS next2_enrolled_at,
        SUM(enrolled_at_quarter) OVER (PARTITION BY enrolled_at_month ORDER BY enrolled_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS enrolled_at_quarter_sum_2_prior
    FROM enriched
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE enrolled_at_month_filled_tercile > (SELECT AVG(enrolled_at_month_filled_tercile) FROM windowed)
),
scored AS (
    -- Derive helper columns
    SELECT *, COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM with_activity
),
unique_rows AS (
    -- Keep the latest row per student_id, grade_status_or_unknown
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at_quarter,
        term_len,
        enrolled_at_month,
        enrolled_at_month_filled,
        enrolled_at_label,
        enrolled_at_quarter_distance,
        enrolled_at_month_filled_tercile,
        next2_enrolled_at,
        enrolled_at_quarter_sum_2_prior,
        grade_status_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, grade_status_or_unknown ORDER BY score DESC, id DESC) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    enrolled_at_quarter,
    term_len,
    enrolled_at_month_filled,
    enrolled_at_label,
    enrolled_at_quarter_distance,
    enrolled_at_month_filled_tercile,
    next2_enrolled_at,
    enrolled_at_quarter_sum_2_prior,
    grade_status_or_unknown,
    COALESCE(grade_status_or_unknown, 'UNKNOWN') AS grade_status_or_unknown_or_unknown
FROM unique_rows
ORDER BY enrolled_at_quarter, id
LIMIT 5
