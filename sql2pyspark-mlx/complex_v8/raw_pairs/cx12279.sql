WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
      AND warehouse_id <= 4
),
per_group AS (
    -- Group-level totals
    SELECT
        move_type,
        warehouse_id,
        SUM(unit_price) FILTER (WHERE operator IS NOT NULL) AS unit_price_operator_is_not_null,
        ROUND(AVG(unit_price), 2) AS avg_unit_price,
        SUM(unit_price) AS total_unit_price
    FROM valid_moves
    GROUP BY 1, 2
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        move_type,
        warehouse_id,
        avg_unit_price,
        COUNT(*) OVER (PARTITION BY move_type, warehouse_id) AS rows_in_move_type_warehouse_id,
        MIN(avg_unit_price) OVER (PARTITION BY move_type, warehouse_id) AS move_type_warehouse_id_min_avg_unit_price,
        NTH_VALUE(avg_unit_price, 2) OVER (ORDER BY avg_unit_price DESC, move_type, warehouse_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_avg_unit_price,
        ROUND(avg_unit_price / NULLIF(SUM(avg_unit_price) OVER (PARTITION BY move_type, warehouse_id), 0), 4) AS avg_unit_price_share_of_move_type_warehouse_id,
        MIN(total_unit_price) OVER (PARTITION BY move_type, warehouse_id ORDER BY avg_unit_price, move_type, warehouse_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_total_unit_price_so_far,
        ROUND(total_unit_price - AVG(total_unit_price) OVER w_move_type_warehouse_id, 2) AS total_unit_price_vs_move_type_warehouse_id_avg
    FROM per_group
    WINDOW w_move_type_warehouse_id AS (PARTITION BY move_type, warehouse_id)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced
    WHERE avg_unit_price > (SELECT AVG(avg_unit_price) FROM sequenced)
),
combined_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.move_type,
        s.warehouse_id,
        s.avg_unit_price,
        s.move_type_warehouse_id_min_avg_unit_price,
        s.min_total_unit_price_so_far,
        w.capacity AS warehouse_capacity
    FROM with_activity s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.capacity <= 11506
),
tagged AS (
    -- Add calculated columns
    SELECT
        move_type,
        warehouse_id,
        avg_unit_price,
        move_type_warehouse_id_min_avg_unit_price,
        min_total_unit_price_so_far,
        LEAST(avg_unit_price, warehouse_capacity) AS least_avg_unit_price_warehouse_capacity,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM combined_details_warehouse
    WHERE min_total_unit_price_so_far > 10500.6
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        move_type,
        warehouse_id,
        move_type_warehouse_id_min_avg_unit_price,
        min_total_unit_price_so_far,
        move_type_slug,
        LEAD(avg_unit_price, 2, 0) OVER (PARTITION BY warehouse_id, move_type ORDER BY move_type_warehouse_id_min_avg_unit_price DESC, move_type, warehouse_id) AS next2_avg_unit_price,
        NTH_VALUE(min_total_unit_price_so_far, 2) OVER (PARTITION BY warehouse_id, move_type ORDER BY move_type_warehouse_id_min_avg_unit_price DESC, move_type, warehouse_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_total_unit_price_so_far,
        MIN(avg_unit_price) OVER (ORDER BY avg_unit_price DESC, move_type, warehouse_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_avg_unit_price_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id, move_type ORDER BY move_type_warehouse_id_min_avg_unit_price ASC NULLS LAST, move_type, warehouse_id), 3) AS move_type_warehouse_id_min_avg_unit_price_percen,
        ROW_NUMBER() OVER w_all_by_avg_unit_price AS rn
    FROM tagged
    WINDOW w_all_by_avg_unit_price AS (ORDER BY avg_unit_price DESC, move_type, warehouse_id)
)
-- Final projection
SELECT
    move_type,
    warehouse_id,
    move_type_warehouse_id_min_avg_unit_price,
    min_total_unit_price_so_far,
    move_type_slug,
    next2_avg_unit_price,
    min_avg_unit_price_so_far,
    rn,
    LOWER(REGEXP_REPLACE(TRIM(move_type_slug), ' ', '_')) AS move_type_slug_slug
FROM sequenced_2
WHERE min_total_unit_price_so_far <= 24000.2
ORDER BY min_total_unit_price_so_far, move_type_slug
