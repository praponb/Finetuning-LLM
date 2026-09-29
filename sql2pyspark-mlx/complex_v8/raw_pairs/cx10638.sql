WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator = 'op_eve'
      AND move_type NOT IN ('OUTBOUND', 'INBOUND')
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code
    FROM cleaned_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(qty, 3) OVER (PARTITION BY warehouse_id, sku_id ORDER BY warehouse_opened_on, id) AS next3_qty,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY warehouse_city), 0), 4) AS unit_price_share_of_warehouse_city,
        ROUND(unit_price - AVG(unit_price) OVER w_warehouse_id_sku_id, 2) AS unit_price_vs_warehouse_id_sku_id_avg,
        DATEDIFF(warehouse_opened_on, LAG(warehouse_opened_on) OVER (ORDER BY qty, id)) AS days_since_prev_warehouse_opened_on,
        DENSE_RANK() OVER (PARTITION BY move_type, sku_id ORDER BY qty DESC, id) AS qty_dense_rank,
        MIN(unit_price) OVER (PARTITION BY warehouse_id, sku_id) AS warehouse_id_sku_id_min_unit_price,
        RANK() OVER w_operator_by_unit_price_ties AS unit_price_rank,
        LAG(unit_price) OVER w_all_by_qty AS prev_unit_price,
        SUM(unit_price) OVER (PARTITION BY operator ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        COUNT(*) OVER (PARTITION BY warehouse_city) AS rows_in_warehouse_city,
        COUNT(*) OVER (PARTITION BY warehouse_id, sku_id) AS rows_in_warehouse_id_sku_id,
        LAST_VALUE(qty) OVER (PARTITION BY move_type, sku_id ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_city)) AS distinct_operator_in_warehouse_city,
        unit_price - LAG(unit_price) OVER (ORDER BY qty, id) AS unit_price_change,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY warehouse_city)) AS distinct_warehouse_id_in_warehouse_city,
        LAG(warehouse_opened_on) OVER (PARTITION BY sku_id, operator ORDER BY moved_at DESC NULLS FIRST, id) AS prev_warehouse_opened_on
    FROM joined
    WINDOW w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id), w_operator_by_unit_price_ties AS (PARTITION BY operator ORDER BY unit_price DESC), w_all_by_qty AS (ORDER BY qty, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE qty_dense_rank = 1
),
summary AS (
    -- Group-level totals
    SELECT
        warehouse_city,
        MAX(qty) AS max_qty,
        COUNT(*) FILTER (WHERE warehouse_wh_code IS NOT NULL) AS n_warehouse_wh_code_is_not_nul,
        ARRAY_SORT(COLLECT_SET(warehouse_city)) AS warehouse_city_list,
        MIN(unit_price_share_of_warehouse_city) AS min_unit_price_share_of_warehouse_city,
        COUNT(*) AS cnt,
        COUNT(DISTINCT operator) AS distinct_operators
    FROM top_rows
    WHERE rows_in_warehouse_id_sku_id > 4
    GROUP BY warehouse_city
    HAVING SUM(unit_price_rank) > 5
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY distinct_operators, n_warehouse_wh_code_is_not_nul ORDER BY distinct_operators) AS distinct_operators_dense_rank,
        MAX(max_qty) OVER (PARTITION BY cnt, distinct_operators) AS cnt_distinct_operators_max_max_qty,
        ROUND(AVG(distinct_operators) OVER (PARTITION BY distinct_operators, n_warehouse_wh_code_is_not_nul ORDER BY distinct_operators, warehouse_city ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS distinct_operators_avg_3_prior
    FROM summary
)
SELECT
    warehouse_city,
    max_qty,
    n_warehouse_wh_code_is_not_nul,
    warehouse_city_list,
    distinct_operators,
    distinct_operators_avg_3_prior
FROM sequenced
WHERE cnt_distinct_operators_max_max_qty <= 3250
