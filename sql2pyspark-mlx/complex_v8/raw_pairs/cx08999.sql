WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        LEAST(unit_price, qty) AS least_unit_price_qty,
        qty + unit_price AS qty_plus_unit_price
    FROM stock_moves
    WHERE unit_price BETWEEN 152.7 AND 292.3
      AND move_type NOT IN ('TRANSFER', 'ADJUST')
),
combined_details_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.qty_plus_unit_price,
        sk.labels AS sku_labels,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost,
        w.capacity AS warehouse_capacity
    FROM base_moves s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        moved_at,
        unit_price,
        sku_labels,
        sku_category,
        sku_unit_cost,
        COUNT(*) OVER (PARTITION BY TO_DATE(moved_at)) AS rows_in_moved_at_to_date,
        SUM(qty_plus_unit_price) OVER (PARTITION BY TO_DATE(moved_at)) AS moved_at_to_date_sum_qty_plus_unit_price,
        LEAD(warehouse_capacity) OVER (ORDER BY unit_price, id) AS next_warehouse_capacity,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        ROUND(AVG(sku_unit_cost) OVER (ORDER BY unit_price, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS sku_unit_cost_avg_1_prior,
        ROUND(AVG(warehouse_capacity) OVER (PARTITION BY sku_id ORDER BY unit_price NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS warehouse_capacity_avg_4,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY unit_price NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        LAST_VALUE(qty_plus_unit_price) IGNORE NULLS OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty_plus_unit_price,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id ORDER BY unit_price NULLS LAST), 3) AS unit_price_percent_rank,
        SUM(unit_price) OVER w_moved_at_to_date AS moved_at_to_date_sum_unit_price,
        ROW_NUMBER() OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at DESC, id) AS moved_at_seq
    FROM combined_details_sku
    WINDOW w_moved_at_to_date AS (PARTITION BY TO_DATE(moved_at)), w_sku_id_warehouse_id_by_moved_2 AS (PARTITION BY sku_id, warehouse_id ORDER BY moved_at DESC, id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_labels,
        sku_unit_cost,
        rows_in_moved_at_to_date,
        moved_at_to_date_sum_qty_plus_unit_price,
        next_warehouse_capacity,
        sku_unit_cost_avg_1_prior,
        warehouse_capacity_avg_4,
        last_known_moved_at_2,
        moved_at_to_date_sum_unit_price,
        NTILE(10) OVER (ORDER BY sku_unit_cost_avg_1_prior ASC NULLS LAST, id) AS sku_unit_cost_avg_1_prior_decile,
        LEAD(moved_at) OVER (PARTITION BY warehouse_id, DATE_TRUNC('MONTH', last_known_moved_at) ORDER BY last_known_moved_at, id) AS next_moved_at,
        LAST_VALUE(sku_unit_cost_avg_1_prior) OVER (ORDER BY sku_unit_cost_avg_1_prior ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sku_unit_cost_avg_1_prior,
        NTH_VALUE(sku_unit_cost_avg_1_prior, 2) OVER (ORDER BY sku_unit_cost_avg_1_prior ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_unit_cost_avg_1_prior,
        FIRST_VALUE(first_known_unit_price) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), sku_category ORDER BY last_known_qty_plus_unit_price ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_first_known_unit_price
    FROM metrics
)
SELECT
    id,
    warehouse_id,
    sku_unit_cost,
    rows_in_moved_at_to_date,
    moved_at_to_date_sum_qty_plus_unit_price,
    next_warehouse_capacity,
    sku_unit_cost_avg_1_prior,
    warehouse_capacity_avg_4,
    last_known_moved_at_2,
    moved_at_to_date_sum_unit_price,
    sku_unit_cost_avg_1_prior_decile,
    next_moved_at,
    second_sku_unit_cost_avg_1_prior,
    first_first_known_unit_price,
    FLOOR(next_warehouse_capacity / 5000) * 5000 AS next_warehouse_capacity_bucket,
    COALESCE(rows_in_moved_at_to_date, 0) AS rows_in_moved_at_to_date_filled
FROM windowed
ORDER BY first_first_known_unit_price, rows_in_moved_at_to_date DESC
