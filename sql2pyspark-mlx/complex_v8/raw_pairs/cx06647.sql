WITH valid AS (
    -- Filter warehouses
    SELECT
        id,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE city IN ('Kuala Lumpur', 'Singapore')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        capacity,
        opened_on,
        ROUND(AVG(capacity) OVER (PARTITION BY city ORDER BY capacity DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS capacity_avg_4,
        MIN(capacity) OVER (PARTITION BY city) AS city_min_capacity,
        DENSE_RANK() OVER w_city_by_capacity_ties AS capacity_dense_rank
    FROM valid
    WINDOW w_city_by_capacity_ties AS (PARTITION BY city ORDER BY capacity DESC)
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.opened_on,
        s.capacity_avg_4,
        s.capacity_dense_rank,
        st.id AS move_id,
        st.sku_id AS move_sku_id,
        st.unit_price AS move_unit_price
    FROM windowed s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.opened_on,
        s.capacity_avg_4,
        s.move_id,
        s.move_sku_id,
        s.move_unit_price,
        st.id AS move_id_2,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price_2,
        st.moved_at AS move_moved_at
    FROM detail_rows_moves s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.id
),
enriched AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.opened_on,
        s.move_id,
        s.move_sku_id,
        s.move_unit_price,
        s.move_id_2,
        s.move_qty,
        s.move_unit_price_2,
        s.move_moved_at,
        sk.weight_kg AS sku_weight_kg
    FROM line_level s
    JOIN skus sk ON sk.id = s.move_sku_id
    WHERE sk.weight_kg > 12.4
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_move_id_move_sku_id_by_move AS move_unit_price_2_rank,
        move_unit_price_2 - LAG(move_unit_price_2) OVER (PARTITION BY move_id, move_sku_id ORDER BY move_unit_price_2 NULLS LAST, id, move_id, move_id_2) AS move_unit_price_2_change,
        NTILE(4) OVER (PARTITION BY move_id, move_sku_id ORDER BY move_unit_price_2 NULLS LAST, id, move_id, move_id_2) AS move_unit_price_2_quartile,
        ROUND(move_unit_price_2 / NULLIF(SUM(move_unit_price_2) OVER w_move_id, 0), 4) AS move_unit_price_2_share_of_move_id
    FROM enriched
    WINDOW w_move_id_move_sku_id_by_move AS (PARTITION BY move_id, move_sku_id ORDER BY move_unit_price_2 NULLS LAST), w_move_id AS (PARTITION BY move_id)
)
SELECT
    id,
    opened_on,
    move_id,
    move_id_2,
    move_qty,
    move_unit_price_2,
    move_moved_at,
    sku_weight_kg,
    move_unit_price_2_rank,
    move_unit_price_2_change,
    GREATEST(sku_weight_kg, move_qty) AS greatest_sku_weight_kg_move_qty
FROM with_windows
ORDER BY greatest_sku_weight_kg_move_qty, move_unit_price_2 DESC
