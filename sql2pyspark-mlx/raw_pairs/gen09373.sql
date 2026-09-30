WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END AS is_kerry
    FROM shipments
    WHERE cost < 68
      AND carrier IS NOT NULL
)
-- Ranking and gap features
SELECT
    id,
    order_id,
    carrier,
    delivered_date,
    cost,
    status,
    is_kerry,
    CUME_DIST() OVER (ORDER BY cost DESC NULLS LAST) AS cost_cume_dist,
    LEAD(cost, 2) OVER (PARTITION BY MONTH(shipped_date), status ORDER BY delivered_date, id) AS next_cost
FROM cleaned;
