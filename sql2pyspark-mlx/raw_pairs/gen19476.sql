WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        TRIM(carrier) AS clean_carrier
    FROM shipments
    WHERE delivered_date >= DATE '2023-06-06'
      AND carrier IS DISTINCT FROM 'Kerry'
)
-- Running and moving aggregates
SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY cost DESC) AS cost_percent_rank,
    AVG(cost) OVER w_part AS cost_avg_2_prior
FROM filtered
WINDOW w_part AS (PARTITION BY YEAR(delivered_date), status ORDER BY delivered_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
