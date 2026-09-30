WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE holder_id >= 15
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status <> 'ACTIVE'
        )
      AND s.holder_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        start_date,
        status,
        MAX(premium) OVER (PARTITION BY status) AS status_max_premium,
        ROUND(premium - AVG(premium) OVER w_status, 2) AS premium_vs_status_avg,
        MAX(premium) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far,
        LAG(premium, 3) OVER (ORDER BY start_date, id) AS prev3_premium,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date), 3) AS start_date_percent_rank
    FROM filtered
    WINDOW w_status AS (PARTITION BY status)
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    status,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(max_premium_so_far) AS total_max_premium_so_far
FROM sequenced
GROUP BY GROUPING SETS ((holder_id, status), (holder_id))
