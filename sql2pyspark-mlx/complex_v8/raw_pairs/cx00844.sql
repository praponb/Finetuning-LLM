WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day,
        LEAST(unit_price, qty) AS least_unit_price_qty,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE qty >= 145
      AND warehouse_id >= 2
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        moved_at,
        least_unit_price_qty,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY moved_at_day) AS moved_at_day_dense_rank,
        ROUND(AVG(least_unit_price_qty) OVER (PARTITION BY sku_id ORDER BY moved_at_day, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS least_unit_price_qty_avg_1_prior
    FROM cleaned_moves
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(least_unit_price_qty / NULLIF(moved_at_day_dense_rank, 0), 3) AS least_unit_price_qty_per_moved_at_day_dense_rank,
        ABS(least_unit_price_qty - 350.3) AS least_unit_price_qty_distance,
        CASE WHEN least_unit_price_qty >= 375.2 THEN 'large' WHEN least_unit_price_qty >= 125.8 THEN 'medium' ELSE 'small' END AS least_unit_price_qty_tier
    FROM windowed
    WHERE least_unit_price_qty_avg_1_prior < 375.2
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM scored
    WHERE least_unit_price_qty_distance > (SELECT MIN(least_unit_price_qty_distance) FROM scored)
)
SELECT id, metric, reading
FROM (
    SELECT id, least_unit_price_qty_distance, least_unit_price_qty
    FROM screened
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (least_unit_price_qty_distance, least_unit_price_qty)
) unp
