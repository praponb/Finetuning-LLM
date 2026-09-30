WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(cost * 0.9, 2) cost_adjusted,
        UPPER(carrier) carrier_upper,
        COALESCE(carrier, 'UNKNOWN') resolved_carrier
    FROM shipments
    WHERE status IN ('DELIVERED', 'LOST', 'IN_TRANSIT')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost_adjusted,
        carrier_upper,
        resolved_carrier,
        LEAST(cost, cost_adjusted) least_cost_cost_adjusted,
        COALESCE(carrier, 'UNKNOWN') carrier_filled
    FROM filtered
    WHERE shipped_date > TO_DATE('2023-09-10')
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC NULLS FIRST, id) AS latest_rank
        FROM valid
    ) ranked
    WHERE latest_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost_adjusted, id) AS rank_in_group
        FROM ranked
    ) ranked
    WHERE rank_in_group = 1
)
-- Running and moving aggregates
SELECT
    *,
    NTILE(3) OVER w_running least_cost_cost_adjusted_bucket,
    AVG(cost_adjusted) OVER w_ordered cost_adjusted_avg_2,
    LAG(least_cost_cost_adjusted, 1) OVER w_running prev_least_cost_cost_adjusted
FROM deduped
WINDOW w_running AS (PARTITION BY order_id ORDER BY least_cost_cost_adjusted, id), w_ordered AS (ORDER BY shipped_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
