WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status
    FROM policies
    WHERE end_date < DATE '2026-08-31'
      AND premium < 3817.5
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY start_date DESC), 3) AS start_date_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY start_date), 3) AS start_date_cume_dist_2,
        NTH_VALUE(start_date, 2) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date
    FROM scoped_policys
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, p.holder_name
    FROM metrics s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    id,
    holder_id,
    product,
    start_date,
    status,
    start_date_cume_dist_2,
    second_start_date,
    CASE WHEN start_date_cume_dist >= 0.8 THEN 'HIGH' WHEN start_date_cume_dist >= 0.4 THEN 'MEDIUM' ELSE 'LOW' END AS start_date_cume_dist_tier
FROM enriched
ORDER BY status, start_date_cume_dist_tier
