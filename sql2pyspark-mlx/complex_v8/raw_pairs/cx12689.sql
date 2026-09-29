WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        CONCAT(city, '#', CAST(capacity AS STRING)) AS city_ref,
        SUBSTRING(TRIM(wh_code), 1, 3) AS wh_code_prefix
    FROM warehouses
    WHERE LOWER(wh_code) LIKE '%l1%'
      AND opened_on IS NOT NULL
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND HOUR(st.moved_at) BETWEEN 8 AND 10
        )
),
deduped AS (
    -- Keep the earliest row per city
    SELECT
        id,
        wh_code,
        city,
        capacity,
        city_ref,
        wh_code_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY capacity, id) AS rn
        FROM with_activity
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(wh_code)) AS wh_code_len,
        CONCAT_WS('-', city_ref, wh_code) AS city_ref_wh_code_label,
        CONCAT_WS(' / ', city, city_ref) AS city_city_ref_label
    FROM deduped
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.capacity,
        s.city_ref,
        s.wh_code_prefix,
        s.wh_code_len,
        s.city_city_ref_label,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.moved_at AS move_moved_at
    FROM scored s
    JOIN stock_moves st ON st.warehouse_id = s.id AND st.moved_at < TIMESTAMP '2025-02-12 14:00:00'
),
joined AS (
    -- Join stock_moves
    SELECT
        s.*,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price_2,
        w.opened_on AS warehouse_opened_on
    FROM with_children s
    JOIN stock_moves st ON st.id = s.move_id
    JOIN warehouses w ON w.id = st.warehouse_id
    WHERE st.qty < 326
)
SELECT id, move_id, field, reading
FROM (
    SELECT id, move_id, move_unit_price_2, move_unit_price
    FROM joined
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (move_unit_price_2, move_unit_price)
) unp
