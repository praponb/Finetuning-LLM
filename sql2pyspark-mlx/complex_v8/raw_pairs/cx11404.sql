WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND moved_at < TIMESTAMP '2025-02-24 10:00:00'
),
combined_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.moved_at_label,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        sk.labels AS sku_labels
    FROM filtered s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(warehouse_opened_on, 2) OVER (PARTITION BY TO_DATE(moved_at), warehouse_city ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_warehouse_city, 0), 4) AS unit_price_share_of_warehouse_city,
        COUNT(*) OVER w_warehouse_city_warehouse_id AS rows_in_warehouse_city_warehouse_id,
        SUM(unit_price) OVER (PARTITION BY TO_DATE(moved_at), warehouse_city ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(moved_at), warehouse_city ORDER BY warehouse_opened_on DESC, id) AS warehouse_opened_on_dense_rank,
        SUM(unit_price) OVER (PARTITION BY operator, sku_id ORDER BY moved_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_sum_6_prior,
        MAX(unit_price) OVER (PARTITION BY warehouse_city, warehouse_id) AS warehouse_city_warehouse_id_max_unit_price,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_city)) AS distinct_operator_in_warehouse_city,
        MIN(unit_price) OVER w_warehouse_city_warehouse_id AS warehouse_city_warehouse_id_min_unit_price,
        MAX(unit_price) OVER (PARTITION BY sku_id, warehouse_city ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far,
        FIRST_VALUE(moved_at) OVER (PARTITION BY TO_DATE(moved_at), warehouse_city ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        LAST_VALUE(warehouse_opened_on) IGNORE NULLS OVER (PARTITION BY YEAR(warehouse_opened_on) ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_warehouse_opened_on,
        LEAD(warehouse_opened_on) OVER (PARTITION BY sku_id, warehouse_city ORDER BY moved_at ASC, id) AS next_warehouse_opened_on
    FROM combined_details_warehouse
    WINDOW w_warehouse_city AS (PARTITION BY warehouse_city), w_warehouse_city_warehouse_id AS (PARTITION BY warehouse_city, warehouse_id)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        warehouse_opened_on,
        warehouse_city,
        sku_labels,
        unit_price_share_of_warehouse_city,
        rows_in_warehouse_city_warehouse_id,
        running_unit_price,
        warehouse_opened_on_dense_rank,
        unit_price_sum_6_prior,
        warehouse_city_warehouse_id_max_unit_price,
        distinct_operator_in_warehouse_city,
        warehouse_city_warehouse_id_min_unit_price,
        first_moved_at,
        last_known_warehouse_opened_on,
        next_warehouse_opened_on,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_operator_in_warehouse_city ORDER BY moved_at DESC), 3) AS moved_at_cume_dist,
        MIN(unit_price) OVER (PARTITION BY MONTH(second_warehouse_opened_on), sku_id) AS second_warehouse_opened_on_month_sku_id_min_unit,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_second_warehouse_opened_on_m, 0), 4) AS unit_price_share_of_second_warehouse_opened_on_m
    FROM with_windows
    WINDOW w_second_warehouse_opened_on_m AS (PARTITION BY MONTH(second_warehouse_opened_on), sku_id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    operator,
    warehouse_city,
    sku_labels,
    unit_price_share_of_warehouse_city,
    rows_in_warehouse_city_warehouse_id,
    running_unit_price,
    unit_price_sum_6_prior,
    distinct_operator_in_warehouse_city,
    warehouse_city_warehouse_id_min_unit_price,
    last_known_warehouse_opened_on,
    moved_at_cume_dist,
    second_warehouse_opened_on_month_sku_id_min_unit,
    LEAST(running_unit_price, unit_price_sum_6_prior) AS least_running_unit_price_unit_price_sum_6_prior
FROM windowed
WHERE running_unit_price < 3000.4
ORDER BY distinct_operator_in_warehouse_city NULLS LAST, last_known_warehouse_opened_on, id
LIMIT 10
