WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        qty + unit_price AS qty_plus_unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND unit_price < 396.4
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        operator,
        unit_price,
        operator_slug,
        qty_plus_unit_price,
        ROUND(AVG(qty_plus_unit_price) OVER (PARTITION BY sku_id, operator), 2) AS sku_id_operator_avg_qty_plus_unit_price,
        qty_plus_unit_price - LAG(qty_plus_unit_price) OVER w_operator_by_unit_price AS qty_plus_unit_price_change,
        COUNT(*) OVER (PARTITION BY sku_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER w_operator_by_unit_price_ties, 3) AS unit_price_cume_dist,
        SUM(qty_plus_unit_price) OVER (PARTITION BY sku_id, operator) AS sku_id_operator_sum_qty_plus_unit_price
    FROM cleaned
    WINDOW w_operator_by_unit_price AS (PARTITION BY operator ORDER BY unit_price DESC NULLS FIRST, id), w_operator_by_unit_price_ties AS (PARTITION BY operator ORDER BY unit_price DESC NULLS FIRST)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        warehouse_id,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        ROUND(SUM(running_count) / NULLIF(SUM(sku_id_operator_avg_qty_plus_unit_price), 0), 3) AS running_count_to_sku_id_operator_avg_qty_plus_un,
        ROUND(STDDEV(sku_id_operator_avg_qty_plus_unit_price), 2) AS stddev_sku_id_operator_avg_qty_plus_unit_price,
        ROUND(SUM(sku_id_operator_sum_qty_plus_unit_price), 2) AS total_sku_id_operator_sum_qty_plus_unit_price
    FROM sequenced
    WHERE qty_plus_unit_price >= 200.8
    GROUP BY warehouse_id
    HAVING SUM(qty_plus_unit_price_change) > -500.0
),
ranked AS (
    -- Previous / next values per key
    SELECT
        warehouse_id,
        operator_list,
        distinct_warehouse_ids,
        running_count_to_sku_id_operator_avg_qty_plus_un,
        stddev_sku_id_operator_avg_qty_plus_unit_price,
        total_sku_id_operator_sum_qty_plus_unit_price,
        ROW_NUMBER() OVER w_all_by_stddev_sku_id_operato AS stddev_sku_id_operator_avg_qty_plus_unit_price_s,
        NTH_VALUE(distinct_warehouse_ids, 2) OVER (PARTITION BY distinct_warehouse_ids, warehouse_id ORDER BY distinct_warehouse_ids DESC, warehouse_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_warehouse_ids,
        SUM(distinct_warehouse_ids) OVER (ORDER BY stddev_sku_id_operator_avg_qty_plus_unit_price, warehouse_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS distinct_warehouse_ids_sum_4,
        LAG(distinct_warehouse_ids) OVER (PARTITION BY distinct_warehouse_ids ORDER BY total_sku_id_operator_sum_qty_plus_unit_price, warehouse_id) AS prev_distinct_warehouse_ids
    FROM per_group
    WINDOW w_all_by_stddev_sku_id_operato AS (ORDER BY stddev_sku_id_operator_avg_qty_plus_unit_price, warehouse_id)
)
SELECT
    warehouse_id,
    operator_list,
    running_count_to_sku_id_operator_avg_qty_plus_un,
    stddev_sku_id_operator_avg_qty_plus_unit_price_s,
    second_distinct_warehouse_ids,
    distinct_warehouse_ids_sum_4,
    prev_distinct_warehouse_ids
FROM ranked
