WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE end_date < DATE '2026-10-23'
),
latest_per_key AS (
    -- Keep the latest row per holder_id, status
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY start_date DESC, id DESC) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with ROLLUP
SELECT
    status,
    holder_id,
    GROUPING_ID() AS grouping_level,
    MAX(premium) AS max_premium
FROM latest_per_key
GROUP BY ROLLUP(status, holder_id)
ORDER BY status NULLS LAST, holder_id NULLS LAST
