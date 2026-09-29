WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        MONTH(end_date) AS end_date_month
    FROM policies
    WHERE (end_date < DATE '2026-07-11' OR start_date IS NOT NULL)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM scoped
    WHERE end_date_month > (SELECT MIN(end_date_month) FROM scoped)
)
SELECT
    end_date_month,
    COALESCE(status, 'ALL') AS status,
    GROUPING_ID() AS grouping_level,
    DATEDIFF(MAX(end_date), MIN(end_date)) AS end_date_span_days,
    COUNT(*) AS n_rows,
    MIN(end_date_month) AS min_end_date_month
FROM screened
GROUP BY GROUPING SETS ((end_date_month, status), (end_date_month), (status), ())
ORDER BY end_date_month NULLS LAST, status NULLS LAST
