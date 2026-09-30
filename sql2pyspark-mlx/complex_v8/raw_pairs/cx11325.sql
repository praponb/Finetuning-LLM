WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref
    FROM policies
    WHERE start_date < DATE '2025-05-28'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(premium) OVER (PARTITION BY YEAR(end_date) ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far,
        SUM(premium) OVER w_status AS status_sum_premium,
        ROW_NUMBER() OVER (PARTITION BY YEAR(end_date) ORDER BY start_date, id) AS rn,
        LAG(end_date, 3) OVER w_all_by_start_date AS prev3_end_date
    FROM recent_policys
    WINDOW w_status AS (PARTITION BY status), w_all_by_start_date AS (ORDER BY start_date ASC, id)
)
-- Subtotals with GROUPING SETS
SELECT
    rn,
    status,
    GROUPING_ID() AS gid,
    MIN(premium) AS min_premium
FROM with_windows
GROUP BY GROUPING SETS ((rn, status), (rn))
