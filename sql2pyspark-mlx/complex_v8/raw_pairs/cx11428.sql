WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE unit_price > 180.0
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN unit_price >= 367.7 THEN 'large' WHEN unit_price >= 161.4 THEN 'medium' ELSE 'small' END AS unit_price_tier,
        TO_DATE(moved_at) AS moved_at_date,
        ABS(qty - 198) AS qty_distance
    FROM base_moves
    WHERE sku_id >= 12
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_unit_price_ties, 3) AS unit_price_cume_dist,
        ROUND(qty_distance - AVG(qty_distance) OVER (PARTITION BY YEAR(moved_at_date), warehouse_id), 2) AS qty_distance_vs_moved_at_date_year_warehouse_id_,
        COUNT(*) OVER (PARTITION BY YEAR(moved_at_date), warehouse_id) AS rows_in_moved_at_date_year_warehouse_id,
        MAX(unit_price) OVER (ORDER BY qty DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_max_6_prior
    FROM tagged
    WINDOW w_all_by_unit_price_ties AS (ORDER BY unit_price NULLS LAST)
)
SELECT id, field, value
FROM (
    SELECT id, unit_price_cume_dist, unit_price_max_6_prior, unit_price
    FROM ranked
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (unit_price_cume_dist, unit_price_max_6_prior, unit_price)
) unp
