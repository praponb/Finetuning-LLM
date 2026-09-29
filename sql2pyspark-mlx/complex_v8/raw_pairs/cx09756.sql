WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE move_type <> 'TRANSFER'
      AND qty <= 314
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        ROUND(unit_price_per_qty / NULLIF(SUM(unit_price_per_qty) OVER (PARTITION BY move_type), 0), 4) AS unit_price_per_qty_share_of_move_type,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(moved_at), warehouse_id ORDER BY moved_at ASC), 3) AS moved_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(moved_at), warehouse_id ORDER BY moved_at ASC, id) AS moved_at_dense_rank,
        RANK() OVER (PARTITION BY TO_DATE(moved_at), warehouse_id ORDER BY moved_at ASC) AS moved_at_rank
    FROM valid_moves
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        moved_at,
        unit_price_per_qty_share_of_move_type,
        moved_at_percent_rank,
        moved_at_dense_rank,
        moved_at_rank,
        LENGTH(TRIM(move_type)) AS move_type_len,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix,
        ROUND(moved_at_dense_rank / NULLIF(moved_at_rank, 0), 3) AS moved_at_dense_rank_per_moved_at_rank
    FROM with_trends
)
-- Final projection
SELECT
    id,
    warehouse_id,
    moved_at,
    unit_price_per_qty_share_of_move_type,
    moved_at_dense_rank,
    move_type_len,
    move_type_prefix,
    moved_at_dense_rank_per_moved_at_rank
FROM scored
WHERE moved_at_percent_rank > 0.2
ORDER BY moved_at_dense_rank DESC
