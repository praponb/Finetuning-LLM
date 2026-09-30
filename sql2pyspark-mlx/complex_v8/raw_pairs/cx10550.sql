WITH prepared AS (
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
    WHERE move_type NOT IN ('OUTBOUND', 'INBOUND')
),
enriched AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.is_active AS sku_is_active,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM prepared s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY qty ASC), 3) AS qty_percent_rank,
        SIZE(COLLECT_SET(sku_category) OVER (PARTITION BY operator)) AS distinct_sku_category_in_operator,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id, sku_category), 2) AS unit_price_vs_warehouse_id_sku_category_avg,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY operator), 2) AS unit_price_vs_operator_avg,
        MAX(qty) OVER w_operator AS operator_max_qty,
        LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at ASC, id) AS prev_moved_at,
        ROUND(qty - AVG(qty) OVER (PARTITION BY warehouse_id, sku_category), 2) AS qty_vs_warehouse_id_sku_category_avg,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, sku_category), 2) AS warehouse_id_sku_category_avg_unit_price,
        NTILE(3) OVER w_warehouse_id_by_qty AS qty_tercile,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_warehouse_id_sku_category, 0), 4) AS unit_price_share_of_warehouse_id_sku_category
    FROM enriched
    WINDOW w_operator AS (PARTITION BY operator), w_warehouse_id_by_qty AS (PARTITION BY warehouse_id ORDER BY qty ASC, id), w_warehouse_id_sku_category AS (PARTITION BY warehouse_id, sku_category)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(operator_max_qty / NULLIF(qty_percent_rank, 0), 3) AS operator_max_qty_per_qty_percent_rank,
        CASE WHEN sku_category IN ('Toys', 'Beauty') THEN 'group_a' WHEN sku_category = 'Food' THEN 'group_b' ELSE 'other' END AS sku_category_group
    FROM with_trends
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_warehouse_id_sku_cate, 3) AS warehouse_id_sku_category_avg_unit_price_cume_di,
        SUM(qty_tercile) OVER (ORDER BY warehouse_id_sku_category_avg_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty_tercile,
        LEAD(qty_percent_rank) OVER w_prev_moved_at_month_sku_cate AS next_qty_percent_rank,
        RANK() OVER (PARTITION BY sku_category ORDER BY distinct_sku_category_in_operator, id) AS distinct_sku_category_in_operator_rank
    FROM tagged
    WINDOW w_all_by_warehouse_id_sku_cate AS (ORDER BY warehouse_id_sku_category_avg_unit_price, id), w_prev_moved_at_month_sku_cate AS (PARTITION BY DATE_TRUNC('MONTH', prev_moved_at), sku_category_group ORDER BY moved_at DESC, id)
),
summary AS (
    -- Summarise per group
    SELECT
        YEAR(moved_at) AS moved_at_year,
        qty_tercile,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        MAX(unit_price_vs_operator_avg) AS max_unit_price_vs_operator_avg,
        COUNT(*) FILTER (WHERE sku_category <> 'Beauty') AS n_sku_category_beauty,
        COUNT(*) FILTER (WHERE operator NOT IN ('op_eve', 'op_ann')) AS n_operator_not_in_op_eve_op_an
    FROM sequenced
    GROUP BY 1, 2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        moved_at_year,
        qty_tercile,
        distinct_sku_ids,
        max_unit_price_vs_operator_avg,
        n_sku_category_beauty,
        MAX(distinct_sku_ids) OVER (PARTITION BY distinct_sku_ids ORDER BY n_sku_category_beauty ASC, moved_at_year, qty_tercile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_distinct_sku_ids_so_far,
        LAST_VALUE(moved_at_year) IGNORE NULLS OVER (PARTITION BY qty_tercile ORDER BY n_operator_not_in_op_eve_op_an, moved_at_year, qty_tercile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at_year,
        LEAD(n_sku_category_beauty) OVER (PARTITION BY qty_tercile ORDER BY n_operator_not_in_op_eve_op_an, moved_at_year, qty_tercile) AS next_n_sku_category_beauty
    FROM summary
)
SELECT
    moved_at_year,
    qty_tercile,
    n_sku_category_beauty,
    last_known_moved_at_year,
    last_known_moved_at_year + moved_at_year AS last_known_moved_at_year_plus_moved_at_year,
    ROUND(distinct_sku_ids / NULLIF(qty_tercile, 0), 3) AS distinct_sku_ids_per_qty_tercile
FROM ranked
ORDER BY moved_at_year DESC, qty_tercile
LIMIT 20
