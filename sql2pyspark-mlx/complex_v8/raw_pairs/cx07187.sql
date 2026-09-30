WITH valid AS (
    -- Filter stock_moves
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
    WHERE operator NOT IN ('op_dew', 'op_ann')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DENSE_RANK() OVER (ORDER BY moved_at DESC) AS moved_at_dense_rank,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        ROUND(CUME_DIST() OVER (ORDER BY moved_at DESC), 3) AS moved_at_cume_dist,
        LAST_VALUE(qty) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty
    FROM valid
),
leaders AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE moved_at_dense_rank = 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(first_known_unit_price) OVER (ORDER BY last_qty DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS first_known_unit_price_max_3,
        ROUND(AVG(first_known_unit_price) OVER w_moved_at_dense_rank, 2) AS moved_at_dense_rank_avg_first_known_unit_price,
        ROUND(last_qty / NULLIF(SUM(last_qty) OVER (PARTITION BY moved_at_dense_rank), 0), 4) AS last_qty_share_of_moved_at_dense_rank,
        NTILE(4) OVER w_all_by_last_qty AS last_qty_quartile,
        last_qty - LAG(last_qty) OVER (PARTITION BY operator, move_type ORDER BY moved_at_cume_dist ASC, id) AS last_qty_change,
        COUNT(*) OVER (PARTITION BY moved_at_dense_rank) AS rows_in_moved_at_dense_rank
    FROM leaders
    WINDOW w_moved_at_dense_rank AS (PARTITION BY moved_at_dense_rank), w_all_by_last_qty AS (ORDER BY last_qty DESC, id)
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        move_type,
        qty,
        moved_at,
        operator,
        moved_at_dense_rank,
        first_known_unit_price,
        first_known_unit_price_max_3,
        moved_at_dense_rank_avg_first_known_unit_price,
        rows_in_moved_at_dense_rank,
        DENSE_RANK() OVER (ORDER BY unit_price) AS unit_price_dense_rank,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type ORDER BY moved_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_3_prior,
        NTILE(4) OVER w_all_by_moved_at_cume_dist AS moved_at_cume_dist_quartile,
        LEAD(moved_at) OVER (ORDER BY unit_price, id) AS next_moved_at
    FROM with_trends
    WINDOW w_all_by_moved_at_cume_dist AS (ORDER BY moved_at_cume_dist DESC, id)
),
deduped AS (
    -- Keep the latest row per move_type
    SELECT
        id,
        move_type,
        qty,
        moved_at,
        operator,
        moved_at_dense_rank,
        first_known_unit_price,
        first_known_unit_price_max_3,
        moved_at_dense_rank_avg_first_known_unit_price,
        rows_in_moved_at_dense_rank,
        unit_price_dense_rank,
        unit_price_avg_3_prior,
        moved_at_cume_dist_quartile,
        next_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY moved_at_dense_rank_avg_first_known_unit_price DESC, id DESC) AS row_num
        FROM with_trends_2
    ) ranked
    WHERE row_num = 1
),
move_type_stats AS (
    -- Avg unit_price_avg_3_prior per move_type
    SELECT move_type, AVG(unit_price_avg_3_prior) AS avg_unit_price_avg_3_prior
    FROM deduped
    GROUP BY move_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price_avg_3_prior,
        ROUND(s.unit_price_avg_3_prior - g.avg_unit_price_avg_3_prior, 2) AS unit_price_avg_3_prior_minus_avg
    FROM deduped s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.unit_price_avg_3_prior > g.avg_unit_price_avg_3_prior
)
SELECT
    id,
    move_type,
    qty,
    moved_at,
    moved_at_dense_rank,
    first_known_unit_price_max_3,
    moved_at_dense_rank_avg_first_known_unit_price,
    rows_in_moved_at_dense_rank,
    unit_price_avg_3_prior,
    next_moved_at,
    avg_unit_price_avg_3_prior,
    unit_price_avg_3_prior_minus_avg,
    CONCAT_WS('-', operator, move_type) AS operator_move_type_label
FROM vs_group
WHERE move_type IS NOT NULL
ORDER BY unit_price_avg_3_prior_minus_avg DESC, qty DESC NULLS FIRST, id
LIMIT 20
