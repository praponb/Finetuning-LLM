WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        TO_DATE(moved_at) AS moved_at_date,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
),
combined_details_sku AS (
    -- Join skus
    SELECT s.*, sk.sku_code
    FROM prepared_moves s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE UPPER(TRIM(sk.sku_code)) LIKE 'E%'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), move_type_len)) AS distinct_sku_id_in_moved_at_month_move_type_len,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY sku_id), 0), 4) AS qty_share_of_sku_id,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), move_type_len), 0), 4) AS qty_share_of_moved_at_month_move_type_len,
        RANK() OVER (PARTITION BY YEAR(moved_at_date) ORDER BY unit_price) AS unit_price_rank,
        LAG(moved_at) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_date), warehouse_id ORDER BY qty DESC NULLS FIRST, id) AS prev_moved_at,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_date), warehouse_id ORDER BY qty DESC NULLS FIRST, id), 3) AS qty_cume_dist,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), move_type_len)) AS distinct_move_type_in_moved_at_month_move_type_l,
        LAG(moved_at) OVER (PARTITION BY YEAR(moved_at_date) ORDER BY unit_price, id) AS prev_moved_at_2,
        DENSE_RANK() OVER (PARTITION BY warehouse_id ORDER BY move_type_len NULLS LAST, id) AS move_type_len_dense_rank,
        MIN(unit_price) OVER (PARTITION BY sku_id) AS sku_id_min_unit_price,
        MAX(unit_price) OVER (PARTITION BY warehouse_id ORDER BY move_type_len NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY moved_at_date DESC, id) AS rn
    FROM combined_details_sku
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_moved_at_month_move_type_len AS (PARTITION BY DATE_TRUNC('MONTH', moved_at), move_type_len)
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE rn <= 5
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        qty - LAG(qty) OVER (PARTITION BY distinct_move_type_in_moved_at_month_move_type_l ORDER BY unit_price DESC, id) AS qty_change,
        SUM(qty_share_of_sku_id) OVER (PARTITION BY rn ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty_share_of_sku_id,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY MONTH(moved_at_date), move_type_len_dense_rank)) AS distinct_move_type_in_moved_at_date_month_move_t
    FROM leaders
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    operator,
    unit_price,
    move_type_len,
    sku_code,
    distinct_sku_id_in_moved_at_month_move_type_len,
    qty_share_of_sku_id,
    qty_share_of_moved_at_month_move_type_len,
    prev_moved_at,
    distinct_move_type_in_moved_at_month_move_type_l,
    prev_moved_at_2,
    move_type_len_dense_rank,
    sku_id_min_unit_price,
    max_unit_price_so_far,
    qty_change,
    running_qty_share_of_sku_id,
    LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
FROM with_windows
ORDER BY prev_moved_at, running_qty_share_of_sku_id
