WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        cost,
        status,
        DAY(shipped_date) AS shipped_date_day,
        COALESCE(carrier, 'OTHER') AS carrier_clean
    FROM shipments
    WHERE shipped_date >= TO_DATE('2023-08-03')
      AND carrier IS NOT DISTINCT FROM 'FedEx'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(shipped_date_day, 1) AS shipped_date_day_rounded,
        cost - shipped_date_day AS cost_minus_shipped_date_day
    FROM valid
    WHERE shipped_date_day = 21
      AND carrier IS DISTINCT FROM 'UPS'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        status,
        carrier_clean,
        shipped_date_day_rounded,
        DENSE_RANK() OVER w_running AS shipped_date_day_rounded_dense_rank,
        AVG(cost_minus_shipped_date_day) OVER w_running_2 AS cost_minus_shipped_date_day_avg_3,
        CUME_DIST() OVER (PARTITION BY carrier ORDER BY cost DESC) AS cost_cume_dist
    FROM cleaned
    WHERE order_id IS NOT NULL
    WINDOW w_running AS (ORDER BY shipped_date_day_rounded DESC), w_running_2 AS (PARTITION BY carrier ORDER BY shipped_date_day, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(status, 1, 4) AS status_prefix
    FROM metrics
    WHERE cost_cume_dist IS NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        shipped_date_day_rounded,
        cost_cume_dist
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date_day_rounded_dense_rank DESC, id) AS row_num
        FROM base
    ) x
    WHERE row_num = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    metric,
    metric_value
FROM (
    SELECT
        id,
        shipped_date_day_rounded,
        cost_cume_dist
    FROM deduped
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (shipped_date_day_rounded, cost_cume_dist)
)
