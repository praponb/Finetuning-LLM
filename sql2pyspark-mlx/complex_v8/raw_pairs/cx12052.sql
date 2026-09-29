WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date
    FROM policies
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_start_date_year_holder_id AS rows_in_start_date_year_holder_id,
        ROUND(AVG(premium) OVER (PARTITION BY YEAR(start_date), holder_id), 2) AS start_date_year_holder_id_avg_premium,
        ROUND(PERCENT_RANK() OVER w_holder_id_by_start_date_ties, 3) AS start_date_percent_rank,
        MAX(premium) OVER (PARTITION BY holder_id ORDER BY start_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_max_7
    FROM scoped
    WINDOW w_start_date_year_holder_id AS (PARTITION BY YEAR(start_date), holder_id), w_holder_id_by_start_date_ties AS (PARTITION BY holder_id ORDER BY start_date)
)
SELECT
    status,
    holder_id,
    ROUND(STDDEV(start_date_percent_rank), 2) AS stddev_start_date_percent_rank
FROM with_windows
GROUP BY ROLLUP(status, holder_id)
ORDER BY status NULLS LAST, holder_id NULLS LAST
