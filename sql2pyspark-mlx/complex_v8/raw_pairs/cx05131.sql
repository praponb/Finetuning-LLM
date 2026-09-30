WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        CASE WHEN qty >= 245 THEN 'HIGH' WHEN qty >= 111 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE qty BETWEEN 91 AND 394
),
enriched AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.qty_tier,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code
    FROM cleaned_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE UPPER(TRIM(w.wh_code)) LIKE 'B%'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(qty) OVER (PARTITION BY sku_id ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS qty_sum_3_prior,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id), 2) AS qty_vs_sku_id_avg,
        ROUND(CUME_DIST() OVER (ORDER BY warehouse_opened_on, id), 3) AS warehouse_opened_on_cume_dist,
        ROUND(qty - AVG(qty) OVER w_warehouse_opened_on_month_op, 2) AS qty_vs_warehouse_opened_on_month_operator_avg,
        MIN(qty) OVER (ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_so_far,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_qty,
        SIZE(COLLECT_SET(operator) OVER w_sku_id) AS distinct_operator_in_sku_id,
        ROUND(AVG(qty) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on), operator), 2) AS warehouse_opened_on_month_operator_avg_qty
    FROM enriched
    WINDOW w_warehouse_opened_on_month_op AS (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on), operator), w_sku_id AS (PARTITION BY sku_id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        operator,
        DATE_TRUNC('MONTH', warehouse_opened_on) AS warehouse_opened_on_month,
        distinct_operator_in_sku_id,
        ROUND(STDDEV(warehouse_opened_on_cume_dist), 2) AS stddev_warehouse_opened_on_cume_dist,
        DATEDIFF(MAX(warehouse_opened_on), MIN(warehouse_opened_on)) AS warehouse_opened_on_span_days
    FROM sequenced
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    operator,
    distinct_operator_in_sku_id,
    stddev_warehouse_opened_on_cume_dist,
    warehouse_opened_on_span_days,
    CASE WHEN operator IN ('op_chan', 'op_ann') THEN 'group_a' WHEN operator = 'op_dew' THEN 'group_b' ELSE 'other' END AS operator_group
FROM stats
WHERE distinct_operator_in_sku_id <= 7
ORDER BY operator_group
