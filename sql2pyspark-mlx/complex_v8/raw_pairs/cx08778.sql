WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty < 355
      AND unit_price <= 367.6
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_unit_price AS unit_price_seq,
        RANK() OVER w_warehouse_id_by_moved_at_tie AS moved_at_rank,
        ROUND(PERCENT_RANK() OVER w_warehouse_id_by_moved_at, 3) AS moved_at_percent_rank,
        LEAD(moved_at) OVER (ORDER BY unit_price, id) AS next_moved_at,
        FIRST_VALUE(unit_price) OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        unit_price - LAG(unit_price) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS unit_price_change,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator, warehouse_id), 0), 4) AS unit_price_share_of_operator_warehouse_id,
        ROUND(unit_price - AVG(unit_price) OVER w_warehouse_id, 2) AS unit_price_vs_warehouse_id_avg,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY operator, warehouse_id), 2) AS unit_price_vs_operator_warehouse_id_avg,
        DENSE_RANK() OVER (ORDER BY unit_price DESC) AS unit_price_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY moved_at), 3) AS moved_at_percent_rank_2,
        MAX(unit_price) OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_max_2_prior
    FROM filtered_moves
    WINDOW w_all_by_unit_price AS (ORDER BY unit_price, id), w_warehouse_id_by_moved_at_tie AS (PARTITION BY warehouse_id ORDER BY moved_at), w_warehouse_id_by_moved_at AS (PARTITION BY warehouse_id ORDER BY moved_at, id), w_warehouse_id AS (PARTITION BY warehouse_id)
)
-- Final projection
SELECT
    id,
    operator,
    unit_price,
    unit_price_seq,
    moved_at_percent_rank,
    next_moved_at,
    first_unit_price,
    unit_price_change,
    unit_price_vs_warehouse_id_avg,
    unit_price_vs_operator_warehouse_id_avg,
    moved_at_percent_rank_2,
    unit_price_max_2_prior,
    unit_price_seq + moved_at_percent_rank_2 AS unit_price_seq_plus_moved_at_percent_rank_2
FROM windowed
ORDER BY id DESC, unit_price_change DESC
