WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        GREATEST(qty, unit_price) AS greatest_qty_unit_price
    FROM stock_moves
    WHERE sku_id <= 9
),
joined_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city,
        sk.category AS sku_category,
        sk.sku_code
    FROM cleaned s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.city <> 'Bangkok'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(warehouse_capacity) OVER (PARTITION BY warehouse_city, YEAR(warehouse_opened_on) ORDER BY unit_price DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS warehouse_capacity_avg_2_prior,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER (PARTITION BY warehouse_city), 2) AS warehouse_capacity_vs_warehouse_city_avg,
        ROUND(unit_price - AVG(unit_price) OVER w_warehouse_city, 2) AS unit_price_vs_warehouse_city_avg,
        LEAD(warehouse_opened_on) OVER (PARTITION BY operator ORDER BY warehouse_opened_on ASC, id) AS next_warehouse_opened_on,
        ROW_NUMBER() OVER w_all_by_unit_price AS unit_price_seq,
        LAG(warehouse_capacity) OVER (PARTITION BY warehouse_city, YEAR(warehouse_opened_on) ORDER BY unit_price DESC, id) AS prev_warehouse_capacity,
        DENSE_RANK() OVER w_operator_by_warehouse_opened_2 AS warehouse_opened_on_dense_rank,
        ROUND(warehouse_capacity / NULLIF(SUM(warehouse_capacity) OVER (PARTITION BY warehouse_city), 0), 4) AS warehouse_capacity_share_of_warehouse_city,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price), 3) AS unit_price_percent_rank,
        DENSE_RANK() OVER (ORDER BY unit_price, id) AS unit_price_dense_rank
    FROM joined_warehouse
    WINDOW w_warehouse_city AS (PARTITION BY warehouse_city), w_all_by_unit_price AS (ORDER BY unit_price, id), w_operator_by_warehouse_opened_2 AS (PARTITION BY operator ORDER BY warehouse_opened_on ASC)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(warehouse_capacity - 4912) AS warehouse_capacity_distance,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj,
        ABS(warehouse_capacity - 11005) AS warehouse_capacity_distance_2
    FROM ranked
    WHERE prev_warehouse_capacity > 11005
)
SELECT
    id,
    warehouse_id,
    sku_id,
    operator,
    unit_price,
    warehouse_opened_on,
    warehouse_capacity,
    sku_category,
    sku_code,
    warehouse_capacity_avg_2_prior,
    warehouse_capacity_vs_warehouse_city_avg,
    unit_price_vs_warehouse_city_avg,
    next_warehouse_opened_on,
    unit_price_seq,
    prev_warehouse_capacity,
    warehouse_opened_on_dense_rank,
    unit_price_percent_rank,
    unit_price_dense_rank,
    warehouse_capacity_distance,
    warehouse_capacity_distance_2,
    CASE WHEN unit_price_seq >= 8 THEN 'A' WHEN unit_price_seq >= 5 THEN 'B' ELSE 'C' END AS unit_price_seq_tier
FROM enriched
WHERE unit_price_dense_rank >= 4
