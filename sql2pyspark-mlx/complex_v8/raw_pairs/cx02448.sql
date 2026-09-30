WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE SIZE(labels) >= 2
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.category,
        s.unit_cost,
        s.weight_kg,
        s.labels,
        s.is_active,
        st.id AS move_id,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at
    FROM recent s
    JOIN stock_moves st ON st.sku_id = s.id
),
exploded AS (
    -- One row per label
    SELECT
        s.id,
        s.category,
        s.unit_cost,
        s.weight_kg,
        s.is_active,
        s.move_id,
        s.move_qty,
        s.move_moved_at,
        t.label
    FROM exploded_detail_moves s
    LATERAL VIEW EXPLODE(s.labels) t AS label
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        move_id,
        move_qty,
        label,
        SUBSTRING(TRIM(label), 1, 3) AS label_prefix,
        COALESCE(move_qty, 0) AS move_qty_filled
    FROM exploded
    WHERE unit_cost <= 277.0
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, move_id, measure, amount
FROM (
    SELECT id, move_id, move_qty, move_qty_filled
    FROM scored
) src
UNPIVOT (
    amount FOR measure IN (move_qty, move_qty_filled)
) unp
