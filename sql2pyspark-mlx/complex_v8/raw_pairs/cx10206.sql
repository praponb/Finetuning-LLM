WITH filtered_moves AS (
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
        MONTH(moved_at) AS moved_at_month,
        DATE_TRUNC('YEAR', moved_at) AS moved_at_year
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-12'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(unit_price) OVER (PARTITION BY move_type, warehouse_id) AS move_type_warehouse_id_min_unit_price,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id, DATE_TRUNC('MONTH', moved_at_year)), 2) AS unit_price_vs_warehouse_id_moved_at_year_month_a,
        RANK() OVER (PARTITION BY MONTH(moved_at_year) ORDER BY moved_at_month DESC, id) AS moved_at_month_rank,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_id, DATE_TRUNC('MONTH', moved_at_year))) AS distinct_move_type_in_warehouse_id_moved_at_year,
        FIRST_VALUE(unit_price) OVER (PARTITION BY TO_DATE(moved_at_year) ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id, DATE_TRUNC('MONTH', moved_at_year))) AS distinct_sku_id_in_warehouse_id_moved_at_year_mo,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at_month ASC NULLS LAST), 3) AS moved_at_month_percent_rank,
        ROUND(qty / NULLIF(SUM(qty) OVER w_move_type, 0), 4) AS qty_share_of_move_type,
        MAX(unit_price) OVER w_move_type AS move_type_max_unit_price
    FROM filtered_moves
    WINDOW w_move_type AS (PARTITION BY move_type)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at_year DESC), 3) AS moved_at_year_percent_rank,
        ROUND(AVG(qty) OVER (ORDER BY moved_at_year DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_3_prior,
        ROUND(move_type_max_unit_price - AVG(move_type_max_unit_price) OVER w_warehouse_id_moved_at_year_m_2, 2) AS move_type_max_unit_price_vs_warehouse_id_moved_a,
        MAX(move_type_max_unit_price) OVER (ORDER BY moved_at_year DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS move_type_max_unit_price_max_3_prior,
        MAX(moved_at_month) OVER (ORDER BY moved_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_moved_at_month_so_far,
        ROUND(moved_at_month_rank - AVG(moved_at_month_rank) OVER (PARTITION BY warehouse_id, MONTH(moved_at_year)), 2) AS moved_at_month_rank_vs_warehouse_id_moved_at_yea,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY YEAR(moved_at), moved_at_month_rank ORDER BY distinct_sku_id_in_warehouse_id_moved_at_year_mo DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price
    FROM windowed
    WINDOW w_warehouse_id_moved_at_year_m_2 AS (PARTITION BY warehouse_id, MONTH(moved_at_year))
),
totals AS (
    -- Group-level totals
    SELECT
        distinct_sku_id_in_warehouse_id_moved_at_year_mo,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_ym,
        TO_DATE(moved_at_year) AS moved_at_year_day,
        ROUND(AVG(moved_at_month), 2) AS avg_moved_at_month,
        ROUND(SUM(moved_at_month_rank_vs_warehouse_id_moved_at_yea) / NULLIF(SUM(move_type_max_unit_price_vs_warehouse_id_moved_a), 0), 3) AS moved_at_month_rank_vs_warehouse_id_moved_at_yea_2
    FROM ranked
    GROUP BY 1, 2, 3
)
SELECT
    distinct_sku_id_in_warehouse_id_moved_at_year_mo,
    moved_at_ym,
    moved_at_month_rank_vs_warehouse_id_moved_at_yea_2,
    DATE_ADD(moved_at_year_day, 365) AS moved_at_year_day_plus_365d,
    FLOOR(avg_moved_at_month / 2) * 2 AS avg_moved_at_month_bucket
FROM totals
