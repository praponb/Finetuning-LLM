WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status
    FROM policies
    WHERE start_date >= DATE '2024-07-19'
      AND holder_id IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        NTILE(4) OVER (PARTITION BY holder_id ORDER BY start_date, id) AS start_date_quartile,
        ROUND(PERCENT_RANK() OVER w_holder_id_by_start_date_ties, 3) AS start_date_percent_rank
    FROM cleaned
    WINDOW w_holder_id_by_start_date_ties AS (PARTITION BY holder_id ORDER BY start_date)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(first_known_start_date, 3) OVER w_start_date_month_holder_id_b AS next3_first_known_start_date,
        MAX(start_date_quartile) OVER (PARTITION BY start_date_quartile) AS start_date_quartile_max_start_date_quartile
    FROM with_trends
    WINDOW w_start_date_month_holder_id_b AS (PARTITION BY DATE_TRUNC('MONTH', start_date), holder_id ORDER BY start_date DESC, id)
)
SELECT
    id,
    holder_id,
    product,
    start_date,
    status,
    first_known_start_date,
    start_date_quartile,
    start_date_percent_rank,
    next3_first_known_start_date,
    start_date_quartile_max_start_date_quartile
FROM with_windows
WHERE YEAR(start_date) = 2025
ORDER BY status, id
LIMIT 5
