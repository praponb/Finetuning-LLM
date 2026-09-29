WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE student_id IS NOT NULL
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, c.level AS course_level
    FROM base_enrollments s
    JOIN courses c ON c.id = s.course_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER w_enrolled_at_month_student_id AS enrolled_at_quartile,
        LAG(days_since_enrolled_at, 3) OVER (PARTITION BY MONTH(enrolled_at), student_id ORDER BY enrolled_at, id) AS prev3_days_since_enrolled_at,
        DENSE_RANK() OVER w_all_by_enrolled_at AS enrolled_at_dense_rank,
        ROUND(AVG(days_since_enrolled_at) OVER (PARTITION BY student_id), 2) AS student_id_avg_days_since_enrolled_at,
        MIN(days_since_enrolled_at) OVER w_student_id AS student_id_min_days_since_enrolled_at,
        LAG(days_since_enrolled_at, 2) OVER (ORDER BY enrolled_at, id) AS prev2_days_since_enrolled_at
    FROM with_details
    WINDOW w_enrolled_at_month_student_id AS (PARTITION BY MONTH(enrolled_at), student_id ORDER BY enrolled_at, id), w_all_by_enrolled_at AS (ORDER BY enrolled_at, id), w_student_id AS (PARTITION BY student_id)
)
-- Subtotals with CUBE
SELECT
    COALESCE(term_or_unknown, 'ALL') AS term_or_unknown,
    enrolled_at_quartile,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS n_rows
FROM ranked
GROUP BY CUBE(term_or_unknown, enrolled_at_quartile)
